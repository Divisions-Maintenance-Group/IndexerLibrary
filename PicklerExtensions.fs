namespace IndexerLibrary.Protobuf.FSharp

open System
open System.Collections.Generic
open Google.Protobuf
open Google.Protobuf.Collections
open MBrace.FsPickler

module UnknownField =
    let createPickler (resolver : IPicklerResolver) =
        let unknownFieldPickler = resolver.Resolve<UnknownFieldSet> ()
        let writer (w : WriteState) (ns : UnknownFieldSet) =
           unknownFieldPickler.Write w "value" Unchecked.defaultof<UnknownFieldSet>
        let reader (r : ReadState) =
           let v = unknownFieldPickler.Read r "value" in v
        Pickler.FromPrimitives(reader, writer)

module RepeatedField =
    let ofSeq (xs: 'a seq) : RepeatedField<'a> =
        let r = RepeatedField<'a>()
        r.AddRange xs
        r

    let toSeq (r: RepeatedField<'a>) : 'a seq = r
    let toArray (r: RepeatedField<'a>) : 'a[] = r |> Seq.toArray

    //let createPickler<'a> (resolver: IPicklerResolver) : Pickler<RepeatedField<'a>> =
    //    let valuePickler = resolver.Resolve<'a[]> ()
    //    let writer (ws: WriteState) (r : RepeatedField<'a>) =
    //        failwith "bang"
    //        let x =
    //            r
    //            |> Seq.toArray
    //            |> valuePickler.Write ws "values"
    //        x
    //    let reader (rs: ReadState) =
    //        //valuePickler.Read rs "values" |> ofSeq
    //        failwith "oh no"
    //        let x = ofSeq Seq.empty
    //        x
    //    Pickler.FromPrimitives(reader, writer)

module MapField =
    let ofDict (xs: IDictionary<'K,'V>) =
        let result = MapField<'K,'V>()
        result.Add xs
        result

    let ofMap (xs: Map<'K,'V>) = ofDict xs

    let ofSeq (xs: seq<'K*'V>) =
        let result = MapField<'K,'V>()
        for (k,v) in xs do
            result[k] <- v
        result

module Pickler =
    /// Adds Protobuf picklers to a custom pickler registry.
    let addToRegistry (registry: CustomPicklerRegistry) =
        registry.RegisterFactory UnknownField.createPickler
        //registry.RegisterFactory RepeatedField.createPickler
        let repeatedFieldTypeDef = typeof<RepeatedField<int>>.GetGenericTypeDefinition()
        registry.DeclareSerializable (fun t -> t.IsGenericType && t.GetGenericTypeDefinition() = repeatedFieldTypeDef)

    /// Creates a pickler registry which constains pickler code for protobuf types.
    let createRegistry () =
        let r = CustomPicklerRegistry()
        r |> addToRegistry
        r
