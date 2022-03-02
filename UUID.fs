namespace Dmg

open System
open System.Text.RegularExpressions

type UUID = private {
    Value: byte[]
} with
    static member New () = 
        {Value = Guid.NewGuid().ToByteArray()}: UUID
    static member New (bytes: byte[]): Result<UUID, string> = 
        if bytes.Length = 16 then
            Ok({Value = bytes}: UUID)
        else
            Error "This byte array is not a valid UUID"

    static member TryParse (str: string): Option<UUID> = 
        try
            Some (UUID.Parse(str))
        with _ ->
            None

    static member Parse (str: string): UUID = 
        let asPairs (source: seq<_>) =
            seq { 
                use iter = source.GetEnumerator() 
                while iter.MoveNext() do
                    let first = iter.Current
                    if iter.MoveNext() then
                        let second = iter.Current 
                        yield (first, second)
            }
        let charToByte (a:char) =
            match a with
            | '0' -> 0uy
            | '1' -> 1uy
            | '2' -> 2uy
            | '3' -> 3uy
            | '4' -> 4uy
            | '5' -> 5uy
            | '6' -> 6uy
            | '7' -> 7uy
            | '8' -> 8uy
            | '9' -> 9uy
            | 'a' -> 10uy
            | 'b' -> 11uy
            | 'c' -> 12uy
            | 'd' -> 13uy
            | 'e' -> 14uy
            | 'f' -> 15uy
            | _ -> failwith "The character you supplied can't be converted"
        let convertUuidStringToBytes uuidString = 
            uuidString 
            |> Seq.toList 
            |> asPairs
            |> Seq.map (fun (a, b) -> (charToByte a <<< 4) + charToByte b)  
            |> Seq.toArray

        let uuidString = str |> String.filter (fun i -> i <> '-')
        match Regex.IsMatch(uuidString, "^[0-9a-f]{32}$") with
        | true -> {Value = uuidString |> convertUuidStringToBytes}: UUID
        | false -> invalidArg (nameof str) "This string is not a valid UUID"
    member this.GetBytes = 
        this.Value
    override this.ToString() = 
        this.GetString
    member this.GetString =
        let byteToChar (a:byte) =
            match a with
            | 0uy  -> '0'  
            | 1uy  -> '1'  
            | 2uy  -> '2'  
            | 3uy  -> '3'  
            | 4uy  -> '4'  
            | 5uy  -> '5'  
            | 6uy  -> '6'  
            | 7uy  -> '7'  
            | 8uy  -> '8'  
            | 9uy  -> '9'  
            | 10uy -> 'a'   
            | 11uy -> 'b'   
            | 12uy -> 'c'   
            | 13uy -> 'd'   
            | 14uy -> 'e'   
            | 15uy -> 'f'   
            | _ -> raise (Exception("The byte you supplied can't be converted"))
        this.Value 
        |> Seq.mapi 
            (fun counter i -> 
                let a = (i &&& 0b11110000uy) >>> 4 |> byteToChar
                let b = i &&& 0b00001111uy |> byteToChar
                match counter with
                | 4
                | 6
                | 8
                | 10 -> $"-{a}{b}"
                | _  -> $"{a}{b}"
            )
        |> String.concat ""
