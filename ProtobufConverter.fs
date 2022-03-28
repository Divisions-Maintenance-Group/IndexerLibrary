namespace Indexer.Protobuf.FSharp

open System
open System.IO
open System.Reflection
open Google.Protobuf

open FSharp.Reflection

type ProtobufConverter(proxyTypesAssembly: Assembly) =
    let fsToCsTypeName (fsClassName: string) =
        // Most types will have exactly the same name in the C#-generated proto code as the F#-generated code,
        // but some will require tweaking. F# likes to append "Module" to module definitions, which are how
        // the nested types get emitted, so we need to remove that suffix.
        fsClassName.Replace("Module+", "+")
    
    member this.MessageToJson<'T when 'T :> IMessage<'T>> (message: 'T, jsonWriter: TextWriter) =
        use stream = new MemoryStream()
        message.WriteTo(stream)
        stream.Position <- 0
        this.BinaryToJson<'T>(stream, jsonWriter)

    member this.MessageToJson<'T when 'T :> IMessage<'T>> (message: 'T) =
        use jsonWriter = new StringWriter()
        this.MessageToJson<'T>(message, jsonWriter)
        jsonWriter.ToString ()

    member _.FindMatchingProtobufType<'T> () =
        let fsMsgType = typeof<'T>
        let csTypeName = fsToCsTypeName fsMsgType.FullName
        match proxyTypesAssembly.GetType csTypeName with
        | null -> raise (ArgumentException($"No type found to match {fsMsgType.FullName}"))
        | t -> t

    member this.BinaryToJson<'T> (inMessageStream: Stream, jsonWriter: TextWriter) =
        let csMsgType = this.FindMatchingProtobufType<'T> ()
        let csMsg = Activator.CreateInstance(csMsgType) :?> IMessage
        csMsg.MergeFrom(inMessageStream)
        let jsFormatter = new JsonFormatter(JsonFormatter.Settings.Default.WithFormatDefaultValues(true))
        jsFormatter.Format(csMsg, jsonWriter)

    member this.JsonToBinary<'T when 'T :> IMessage<'T>> (jsonReader: TextReader, outMessageStream: Stream) =
        let csMsgType = this.FindMatchingProtobufType<'T> ()
        if FSharpType.IsRecord csMsgType then
            invalidArg (nameof<'T>) "Message type should be an F# type"

        let jsParser = new JsonParser(JsonParser.Settings.Default)
        let descriptorProp = csMsgType.GetProperty("Descriptor", BindingFlags.Public ||| BindingFlags.Static)
        let md = descriptorProp.GetValue(null) :?> Google.Protobuf.Reflection.MessageDescriptor
        // JSON -> C# proxy type -> proto binary
        let csMsg = jsParser.Parse (jsonReader, md)
        csMsg.WriteTo outMessageStream

    member this.JsonToMessage<'T when 'T :> IMessage<'T>> (makeT: unit -> 'T, jsonReader: TextReader) =
        // JSON (-> C# proxy type) -> proto binary -> proto F#
        use msgStream = new MemoryStream()
        this.JsonToBinary<'T>(jsonReader, msgStream)
        msgStream.Position <- 0
        let fsMsg = makeT ()
        fsMsg.MergeFrom msgStream
        fsMsg

    member this.JsonToMessage<'T when 'T :> IMessage<'T>> (makeT: unit -> 'T, json: string) =
        use jsonReader = new StringReader(json)
        this.JsonToMessage<'T>(makeT, jsonReader)