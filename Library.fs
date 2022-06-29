namespace IndexerLibrary

open System
open System.Net
open System.Collections.Generic
open System.Linq
open RocksDbSharp
open MBrace.FsPickler
open FSharpPlus
open System.Text.RegularExpressions
open Indexer.Topics

type RepeatedField<'a> = Google.Protobuf.Collections.RepeatedField<'a>
type MapField<'TKey,'TValue> = Google.Protobuf.Collections.MapField<'TKey,'TValue>

module Kafka = 
    open Confluent.Kafka;

    type Kafka(bootstrapServers: string, topic: string) = 
        let bootstrapServers = bootstrapServers
        let topic = topic
        let pConfig = ProducerConfig()
        do
            pConfig.BootstrapServers <- bootstrapServers ///"host1:9092,host2:9092"
            pConfig.ClientId <- UUID.New().ToString()
            pConfig.BatchSize <- 16384
            pConfig.LingerMs <- 100
            let r = System.Text.RegularExpressions.Regex(":9092$")
            let isPlainText = r.Match(bootstrapServers).Success
            if not isPlainText then pConfig.SecurityProtocol <- Confluent.Kafka.SecurityProtocol.Ssl
        let producer = ProducerBuilder<byte[], byte[]>(pConfig).Build()

        member self.getCurrentHeadOffset() = 
            let cConfig = new ConsumerConfig()
            cConfig.BootstrapServers <- bootstrapServers//"host1:9092,host2:9092"
            cConfig.GroupId <- UUID.New().ToString()
            cConfig.AutoOffsetReset <- AutoOffsetReset.Earliest
            let r = System.Text.RegularExpressions.Regex(":9092$")
            let isPlainText = r.Match(bootstrapServers).Success
            if not isPlainText then cConfig.SecurityProtocol <- Confluent.Kafka.SecurityProtocol.Ssl
            use consumer = ConsumerBuilder<byte[], byte[]>(cConfig).Build()
            consumer.Subscribe(topic)
            let current_offset_plus_one = (consumer.QueryWatermarkOffsets (Confluent.Kafka.TopicPartition(topic, 0), System.TimeSpan.FromMilliseconds(5000))).High.Value
            max 0L (current_offset_plus_one - 1L)

        member self.readWholeStream(lineProcessor: (UUID -> byte[] -> int64 -> unit)) = 
            let cConfig = new ConsumerConfig()
            cConfig.BootstrapServers <- bootstrapServers//"host1:9092,host2:9092"
            cConfig.GroupId <- UUID.New().ToString()
            cConfig.AutoOffsetReset <- AutoOffsetReset.Earliest
            let r = System.Text.RegularExpressions.Regex(":9092$")
            let isPlainText = r.Match(bootstrapServers).Success
            if not isPlainText then cConfig.SecurityProtocol <- Confluent.Kafka.SecurityProtocol.Ssl
            use consumer = ConsumerBuilder<byte[], byte[]>(cConfig).Build()
            consumer.Subscribe(topic)
            let current_offset_plus_one = (consumer.QueryWatermarkOffsets (Confluent.Kafka.TopicPartition(topic, 0), System.TimeSpan.FromMilliseconds(5000))).High.Value
            let mutable result = consumer.Consume(5000)
            let mutable exit = false
            if result <> null then
                while current_offset_plus_one - 1L >= result.Offset.Value && not exit do
                    let key = 
                        match (UUID.New result.Message.Key) with
                        | Ok(x) -> x | Error(x) -> failwith "UUID wasn't correct"
                    let value = result.Message.Value
                    let offset = result.Offset.Value
                    lineProcessor key value offset
                    if current_offset_plus_one - 1L > result.Offset.Value then 
                        result <- consumer.Consume()
                    else
                        exit <- true
            consumer.Close();

        member self.writeLineToStream(uuid: UUID, line: byte[]) = 
            async {
                let uuidstuff = uuid.GetBytes
                let! result = producer.ProduceAsync (
                    topic, 
                    new Message<byte[], byte[]>(Key = uuidstuff, Value = line)) |> Async.AwaitTask
                return result.Offset.Value
            }



module Indexer = 
   type Upsert<'Key, 'Value> = {
      Key: 'Key
      Value: Option<'Value>
   }

   type Delta<'Key, 'Value> = {
       Key: 'Key
       PreviousValue: Option<'Value>
       NextValue: Option<'Value>
   }

   type BatchableUpsert<'Key, 'Value> = 
      |Item of Upsert<'Key, 'Value>
      |EndMarker

   type IKeyable<'Key> =
      abstract GetKey: unit -> 'Key
      abstract GetKeyBytes: unit -> byte[]
      abstract Compare: IKeyable<'Key> -> int
      abstract LowestPossibleKeyInRange: unit -> IKeyable<'Key>

   type Subject<'T> () =
      let sync = obj()
      let mutable stopped = false
      let observers = System.Collections.Generic.List<IObserver<'T>>()
      let iter f = observers |> Seq.iter f
      let onCompleted () =
         if not stopped then
            stopped <- true
            iter (fun observer -> observer.OnCompleted())
      let onError ex () =
         if not stopped then
            stopped <- true
            iter (fun observer -> observer.OnError(ex))
      let next value () =
         if not stopped then
            iter (fun observer -> observer.OnNext(value))
      let remove observer () =
         observers.Remove observer |> ignore
      member x.Next value = lock sync <| next value
      member x.Error ex = lock sync <| onError ex
      member x.Completed () = lock sync <| onCompleted
      interface IObserver<'T> with
         member x.OnCompleted() = x.Completed()
         member x.OnError ex = x.Error ex
         member x.OnNext value = x.Next value
      interface IObservable<'T> with
         member this.Subscribe(observer:IObserver<'T>) =
            observers.Add observer
            { new IDisposable with
               member this.Dispose() =
                  lock sync <| remove observer
            }

   type IGettable<'Key, 'T> = 
      abstract Get: 'Key -> 'Key -> Option<Snapshot> -> IEnumerable<Upsert<'Key, 'T>>

   type GettableSubject<'Key, 'Value, 'PipelineOutputType> (performGetFunction: 'Key -> 'Key -> IEnumerable<Upsert<'Key, 'Value>>) =
      inherit Subject<'PipelineOutputType>()
      member x.Get key1 key2 = (x :> IGettable<'Key, 'Value>).Get key1 key2 
      interface IGettable<'Key, 'Value> with
         member this.Get (key1: 'Key) (key2: 'Key) (?snapshot: Snapshot) : IEnumerable<Upsert<'Key,'Value>> = 
            performGetFunction key1 key2
   
   type IPartiallySubscribable<'Key, 'T> = 
      abstract PartiallySubscribe: 'Key -> 'Key -> (Upsert<'Key, 'T> seq -> unit) -> IDisposable

   type PartiallySubscribableSubject<'Key, 'Value, 'PipelineOutputType> 
      (registerPartialSubscriptionFunction: 'Key -> 'Key -> (Upsert<'Key, 'Value> seq -> unit) -> IDisposable, performGetFunction: 'Key -> 'Key -> Option<Snapshot> -> IEnumerable<Upsert<'Key, 'Value>>) = 
      inherit Subject<'PipelineOutputType>()
      member x.PartiallySubscribe key1 key2 f = (x :> IPartiallySubscribable<'Key, 'Value>).PartiallySubscribe key1 key2 f 
      interface IPartiallySubscribable<'Key, 'Value> with
         member this.PartiallySubscribe (key1: 'Key) (key2: 'Key) (f: Upsert<'Key, 'Value> seq -> unit) : IDisposable = 
            registerPartialSubscriptionFunction key1 key2 f
      member x.Get key1 key2 snapshot = (x :> IGettable<'Key, 'Value>).Get key1 key2 snapshot 
      interface IGettable<'Key, 'Value> with
         member this.Get (key1: 'Key) (key2: 'Key) (?snapshot: Snapshot) : IEnumerable<Upsert<'Key,'Value>> = 
            performGetFunction key1 key2 snapshot

   type IIndex<'Key, 'T> = 
      abstract Get: IKeyable<'Key> -> Option<'T>
      abstract Put: IKeyable<'Key> -> Option<'T> -> unit
      abstract WriteBatchPut: IKeyable<'Key> -> Option<'T> -> unit
      abstract Range: IKeyable<'Key> -> IKeyable<'Key> -> Option<IKeyable<'Key> -> IKeyable<'Key> -> int> -> seq<Upsert<IKeyable<'Key>, 'T>>
      abstract RangeUsingSnapshot: IKeyable<'Key> -> IKeyable<'Key> -> Option<IKeyable<'Key> -> IKeyable<'Key> -> int> -> Snapshot -> seq<Upsert<IKeyable<'Key>, 'T>>
      abstract GetAll: unit -> list<Upsert<IKeyable<'Key>, 'T>>
      abstract GetAllAsSequence: unit -> seq<Upsert<IKeyable<'Key>, 'T>>

   let cache = PicklerCache.FromCustomPicklerRegistry (IndexerLibrary.Protobuf.FSharp.Pickler.createRegistry ())
   let bs = FsPickler.CreateBinarySerializer(picklerResolver = cache)


   type UUIDKey =
      {Key: UUID}
      interface IKeyable<UUID> with
         member this.GetKey() = this.Key
         member this.GetKeyBytes() = this.Key.GetBytes
         member this.Compare (other: IKeyable<UUID>) = compare this.Key (other.GetKey())
         member this.LowestPossibleKeyInRange() = 
            {UUIDKey.Key = UUID.TryParse("00000000-0000-0000-0000-000000000000").Value}

   type DateKey =
      {Key: DateTime}
      interface IKeyable<DateTime> with
         member this.GetKey() = this.Key
         member this.GetKeyBytes() = 
            let bytes = this.Key.Ticks |> BitConverter.GetBytes
            if BitConverter.IsLittleEndian then Array.Reverse(bytes)
            bytes
         member this.Compare(other: IKeyable<DateTime>) = compare this.Key (other.GetKey())
         member this.LowestPossibleKeyInRange() = 
            {DateKey.Key = DateTime.MinValue}

   type IntKey = 
      {Key: int}
      interface IKeyable<int> with
         member this.GetKey() = this.Key
         member this.GetKeyBytes() = 
            let bytes = (if this.Key < 0 then ((uint this.Key) &&& uint Int32.MaxValue) else ((uint this.Key) ||| uint Int32.MinValue)) |> BitConverter.GetBytes
            if BitConverter.IsLittleEndian then Array.Reverse(bytes)
            bytes
         member this.Compare (other: IKeyable<int>) = compare this.Key (other.GetKey())
         member this.LowestPossibleKeyInRange() = 
            {IntKey.Key = Int32.MinValue}

   type StringKey =
      {Key: string}
      interface IKeyable<string> with
         member this.GetKey() = this.Key
         member this.GetKeyBytes() = this.Key |> String.truncate 255 |> String.padRight 256 |> bs.Pickle 
         member this.Compare (other: IKeyable<string>) = compare this.Key (other.GetKey())
         member this.LowestPossibleKeyInRange() = 
            {StringKey.Key = ""}

   type CompoundKey<'T1, 'T2> = 
      {Key1: IKeyable<'T1>; Key2: IKeyable<'T2>}
      interface IKeyable<CompoundKey<'T1,'T2>> with
         member this.GetKey() = {Key1 = this.Key1; Key2 = this.Key2}
         member this.GetKeyBytes() = [this.Key1.GetKeyBytes(); this.Key2.GetKeyBytes()] |> Seq.concat |> Seq.toArray
         member this.Compare (other: IKeyable<CompoundKey<'T1, 'T2>>) = 
            if this.Key1.Compare(other.GetKey().Key1) > 0 then
               1
            else if this.Key1.Compare(other.GetKey().Key1) < 0 then
               -1
            else
               if this.Key2.Compare(other.GetKey().Key2) > 0 then
                  1
               else if this.Key2.Compare(other.GetKey().Key2) < 0 then
                  -1
               else
                  0
         member this.LowestPossibleKeyInRange() = 
            {Key1 = this.Key1.LowestPossibleKeyInRange(); Key2 = this.Key2.LowestPossibleKeyInRange()}

   let mutable writeCounter = 0L
   let mutable getCounter = 0L
   let mutable rangeCounter = 0L
   let mutable writeBatchCounter = 0L
   let mutable reduceCounter = 0L
   let mutable testCounter = 0L

   let rocksStopwatch = System.Diagnostics.Stopwatch()
   let globalStopwatch = System.Diagnostics.Stopwatch()
   let primaryStopwatch = System.Diagnostics.Stopwatch()
   let secondaryStopwatch = System.Diagnostics.Stopwatch()
   let mergeStopwatch = System.Diagnostics.Stopwatch()
   let reduceStopwatch = System.Diagnostics.Stopwatch()
   let reduceGroupKeyStopwatch = System.Diagnostics.Stopwatch()
   let reduceRangeStopwatch = System.Diagnostics.Stopwatch()
   let reduceFoldStopwatch = System.Diagnostics.Stopwatch()
   let reduceWriteBatchStopwatch = System.Diagnostics.Stopwatch()
   let reduceRamindexStopwatch = System.Diagnostics.Stopwatch()
   let testStopwatch = System.Diagnostics.Stopwatch()

   globalStopwatch.Start()

   type RocksIndex<'Key, 'T>(db: RocksDb, wb: WriteBatch, columnFamilyName: string) = 
      let cf = db.GetColumnFamily(columnFamilyName)

      member private this.get (key: IKeyable<'Key>): Option<'T> = 
         rocksStopwatch.Start()
         getCounter <- getCounter + 1L
         try 
            let key = key.GetKeyBytes()
            match db.Get(key, cf) with
            | null -> None
            | bytes ->
               let (_, gotValue): IKeyable<'Key> * 'T = bs.UnPickle bytes
               rocksStopwatch.Stop()
               Some gotValue
         with e ->
            raise (Exception("You probably need to destroy your old rocksdb indexes (they are likely out of date)", e))
      member private this.put (key: IKeyable<'Key>) (value: Option<'T>): unit = 
         rocksStopwatch.Start()
         writeCounter <- writeCounter + 1L
         let pkey = key.GetKeyBytes()
         let returnValue = 
            match value with
            | Some x -> db.Remove(pkey, cf)
                        db.Put(pkey, bs.Pickle((key, x)), cf)
            | None -> db.Remove(pkey, cf)
         rocksStopwatch.Stop()
         returnValue
      member private this.writeBatchPut (key: IKeyable<'Key>) (value: Option<'T>): unit = 
         rocksStopwatch.Start()
         writeBatchCounter <- writeBatchCounter + 1L
         let pkey = key.GetKeyBytes()
         let returnValue = 
            match value with
            | Some x -> wb.Delete(pkey, cf) |> ignore
                        wb.Put(pkey, bs.Pickle((key, x)), cf) |> ignore
            | None -> wb.Delete(pkey, cf) |> ignore
         rocksStopwatch.Stop()
         returnValue
      member private this.getall() = 
         use iter = (db.NewIterator(cf))
         iter.SeekToFirst() |> ignore
         let sequence = iter |> Seq.unfold (fun iter -> 
                                                if iter.Valid () then
                                                   let (key: IKeyable<'Key>, value: 'T) = bs.UnPickle (iter.Value())
                                                   Some ({Key = key; Value = Some value}, iter.Next())
                                                else
                                                   None
                                             )
                              |> Seq.toList
         sequence
      member private this.getallAsSequence() = 
         use iter = (db.NewIterator(cf))
         iter.SeekToFirst() |> ignore
         let sequence = iter |> Seq.unfold (fun iter -> 
                                                if iter.Valid () then
                                                   let (key: IKeyable<'Key>, value: 'T) = bs.UnPickle (iter.Value())
                                                   Some ({Key = key; Value = Some value}, iter.Next())
                                                else
                                                   None
                                             )
         sequence
      member private this.range (startKey:IKeyable<'Key>) (endKey:IKeyable<'Key>) (comparer: Option<IKeyable<'Key> -> IKeyable<'Key> -> int>) (snapshot: Option<Snapshot>) : IEnumerable<Upsert<IKeyable<'Key>, 'T>> = 
         rocksStopwatch.Start()
         rangeCounter <- rangeCounter + 1L
         let modefiedComparer (comparer: Option<IKeyable<'Key> -> IKeyable<'Key> -> int>) (x:IKeyable<'Key>) (y:IKeyable<'Key>) = 
            match comparer with
            | Some f ->
               f x y
            | None ->
               x.Compare(y)
         let modifiedComparer = modefiedComparer comparer

         let startKeybytes = startKey.GetKeyBytes()
         use iter = match snapshot with
                    | Some s -> (db.NewIterator(cf, ReadOptions().SetSnapshot(s)))
                    | None -> (db.NewIterator(cf))
         iter.Seek(startKeybytes) |> ignore
         let getKey () = 
            let (key: IKeyable<'Key>, value: 'T) = bs.UnPickle (iter.Value())
            key
//         let rec iterateBackwardsToStart startKey (iter: Iterator): unit =
//            if iter.Valid() then
//               if (compary (getKey()) startKey) >= 0 then 
//                  iter.Prev() |> ignore
//                  iterateBackwardsToStart startKey iter
//         iterateBackwardsToStart startKey iter
//         if iter.Valid() then
//            iter.Next() |> ignore
//         else
//            iter.SeekToFirst() |> ignore
//            if iter.Valid() then
//               if (compary (getKey()) startKey) <> 0 then 
//                  iter.Seek(startKeybytes) |> ignore

         let sequence = iter |> Seq.unfold (fun iter -> 
                                                if iter.Valid () then
                                                   let (key: IKeyable<'Key>, value: 'T) = bs.UnPickle (iter.Value())
                                                   if modifiedComparer key endKey <= 0 then
                                                      Some ({Key = key; Value = Some value}, iter.Next())
                                                   else
                                                      None
                                                else
                                                   None
                                             )
                           |> Seq.toList
         rocksStopwatch.Stop()
         sequence
      interface IIndex<'Key, 'T> with
         member this.Get key = 
            this.get key
         member this.Put key value = 
            this.put key value
         member this.WriteBatchPut key value = 
            this.writeBatchPut key value
         member this.Range startKey endKey ?comparer = 
            this.range startKey endKey comparer None
         member this.RangeUsingSnapshot startKey endKey ?comparer snapshot = 
            this.range startKey endKey comparer (Some snapshot)
         member this.GetAll () = 
            let something = this.getall()
            something
         member this.GetAllAsSequence () = 
            let something = this.getallAsSequence()
            something
            
   type RamIndex<'Key, 'T>() = // when 'Key : comparison>() =
      let dict = new SortedDictionary<IKeyable<'Key>, 'T>(ComparisonIdentity.FromFunction(fun (a: IKeyable<'Key>) b -> a.Compare(b) ))

      member this.getAll = 
         dict.AsEnumerable
      member private this.get (key: IKeyable<'Key>): Option<'T> = 
         try 
            Some dict.[key]
         with
         | :? System.Collections.Generic.KeyNotFoundException -> None
      member private this.Put (key: IKeyable<'Key>) (value: Option<'T>): unit = 
         match value with
         | Some x -> dict.Remove(key) |> ignore
                     dict.Add(key, x)
         | None -> dict.Remove(key) |> ignore
      member private this.Range (startKey:IKeyable<'Key>) (endKey:IKeyable<'Key>) comparer : IEnumerable<Upsert<IKeyable<'Key>,'T>> = 
         let modefiedComparer (comparer: Option<IKeyable<'Key> -> IKeyable<'Key> -> int>) (x:IKeyable<'Key>) (y:IKeyable<'Key>) = 
            match comparer with
            | Some f ->
               f x y
            | None ->
               x.Compare(y)
         let modifiedComparer = modefiedComparer comparer
         let whatever = dict.SkipWhile(fun k -> (modifiedComparer k.Key startKey) < 0 )
                           .TakeWhile(fun k -> (modifiedComparer k.Key endKey) <= 0 )
                           .Select<KeyValuePair<IKeyable<'Key>, 'T>, Upsert<IKeyable<'Key>, 'T>>( fun item -> { Key = item.Key; Value = Some item.Value})
         whatever
      interface IIndex<'Key, 'T> with
         member this.Get key = 
            this.get key
         member this.Put key value = 
            this.Put key value
         member this.WriteBatchPut key value = 
            ()
         member this.Range startKey endKey ?comparer = 
            this.Range startKey endKey comparer
         member this.RangeUsingSnapshot startKey endKey ?comparer snapshot = 
            []
         member this.GetAll () =
            this.getAll() |> Seq.map (fun i -> {Key = i.Key; Value = Some i.Value}) |> Seq.toList
         member this.GetAllAsSequence () =
            this.getAll() |> Seq.map (fun i -> {Key = i.Key; Value = Some i.Value})

   type MergeValues<'Value> = {
      Left: Option<'Value>
      Right: Option<'Value>
   }

   let ts = new System.Threading.CancellationTokenSource()
   type TransactionMessage = 
      | Action of a: (unit -> unit)
      | GetSnapshot of AsyncReplyChannel<Snapshot>
   let createTransactionProcessor (db: RocksDb) = 
      MailboxProcessor.Start(fun inbox ->
         let token = ts.Token
         async {     
            while not ts.IsCancellationRequested do
               let! msg = inbox.Receive()
               if not ts.IsCancellationRequested then
                  match msg with
                  | Action a -> a ()
                  | GetSnapshot(reply) -> reply.Reply(db.CreateSnapshot())
         }
      )
   
   let getSnapshot (transactionProcessor: MailboxProcessor<TransactionMessage>) = 
      transactionProcessor.PostAndReply(fun rc -> GetSnapshot rc)

   let testSource (): Subject<Upsert<IKeyable<'Key>, 'Value> seq> = 
           let s = Subject<Upsert<IKeyable<'Key>, 'Value> seq>()
           s
   
   let getNumberOfPartitions (topicName: string) (hostAndPort: string) : int = 
      let r = Regex(":9092$")
      let isPlainText = r.Match(hostAndPort).Success
      if isPlainText then
         Confluent.Kafka.AdminClientBuilder( Confluent.Kafka.AdminClientConfig( BootstrapServers = hostAndPort )).Build()
            .GetMetadata(TimeSpan.FromSeconds(20))
            .Topics.SingleOrDefault(fun i -> i.Topic = topicName)
            .Partitions.Count
      else
         Confluent.Kafka.AdminClientBuilder( Confluent.Kafka.AdminClientConfig( BootstrapServers = hostAndPort, SecurityProtocol = Confluent.Kafka.SecurityProtocol.Ssl)).Build()
            .GetMetadata(TimeSpan.FromSeconds(20))
            .Topics.SingleOrDefault(fun i -> i.Topic = topicName)
            .Partitions.Count
   
   type SourceNodePosition = string * int * int64

   // hostAndPort should look something like kafka:9093 or localhost:9092 or something like that
   let sourceNode 
      (wb: WriteBatch)
      (db: RocksDb) 
      (rocksDbName) 
      (topicName: string) 
      (hostAndPort: string) 
      (transactionProcessor: MailboxProcessor<TransactionMessage>)
      : IObservable<seq<Upsert<IKeyable<UUID>, 'Value>> * SourceNodePosition> = 

         let numPartitions = getNumberOfPartitions (topicName) (hostAndPort)
         let partitionNumbers = seq { 0 .. (numPartitions-1) }

         let index = (new RocksIndex<string, int64>(db, wb, rocksDbName) :> IIndex<string, int64>) 
         let observable = Subject<Upsert<IKeyable<UUID>, 'Value> seq * SourceNodePosition>()

         partitionNumbers |> Seq.iter (fun partitionNumber -> 
            async {
               let configedPosition = index.Get {StringKey.Key = $"storedPosition-{partitionNumber}"}
               try
                  let kafkaServerConnection = new KafkaServerConnection(hostAndPort)
                  let stateTopic = TopicDefinition.CreateOptional<'Value> (topicName, StateTopic) 
                  let topicNamePartitionOffset = Option.map (fun i -> (topicName, Partition partitionNumber, Offset (i + 1L))) configedPosition
                  let! topic = kafkaServerConnection.OpenReaderAsync(stateTopic, partitionNumber, ?startPosition = topicNamePartitionOffset)

                  let mutable x = 0
                  while true do
                        let! result = topic.Read ()
                        let position = 
                           match result.position with
                           | (_, _, Offset(x)) -> x
                           | _ -> 0L

                        while transactionProcessor.CurrentQueueLength > 1000 do
                           do! Async.Sleep(100)
                        transactionProcessor.Post (Action (fun () -> 
                           observable.Next ([{Key = {UUIDKey.Key = result.key}; Value = result.value}], (topicName, partitionNumber, position))
                           db.Write(wb)
                           wb.Clear() |> ignore
                           match result.position with
                           | (_, _, Offset offset) -> index.Put {StringKey.Key = $"storedPosition-{partitionNumber}"} (Some offset)
                           | _ -> printfn "The offset has been set to an invalid value (probaly a sentinal)"
                           if x % 100 = 0 then 
                              printfn "partitionNumber = %A, stored: %A %A %A %A %A %A %A %A" partitionNumber x writeCounter getCounter rangeCounter writeBatchCounter reduceCounter testCounter result.key
                              printfn "stopwatches: global: %A merge: %A rocks: %A reduce: %A primary: %A secondary: %A rgkey: %A rrange: %A rfold: %A rwb: %A rramindex: %A testStopwatch: %A" 
                                 globalStopwatch.ElapsedMilliseconds 
                                 mergeStopwatch.ElapsedMilliseconds 
                                 rocksStopwatch.ElapsedMilliseconds 
                                 reduceStopwatch.ElapsedMilliseconds 
                                 primaryStopwatch.ElapsedMilliseconds 
                                 secondaryStopwatch.ElapsedMilliseconds
                                 reduceGroupKeyStopwatch.ElapsedMilliseconds
                                 reduceRangeStopwatch.ElapsedMilliseconds
                                 reduceFoldStopwatch.ElapsedMilliseconds
                                 reduceWriteBatchStopwatch.ElapsedMilliseconds
                                 reduceRamindexStopwatch.ElapsedMilliseconds
                                 testStopwatch.ElapsedMilliseconds
                           x <- x + 1
                        ))
               with e ->
                  printfn "%A" e
                  raise e
            } |> Async.Start
         )
         observable

   module HelperFunctions = 
      let sendInitialGetToSubscriber key1 key2 (comparer: Option<IKeyable<'Key> -> IKeyable<'Key> -> int>) (dict: IIndex<_,_>) (observer: Subject<Upsert<_, _> seq>) = 
         let upserts = dict.Range key1 key2 comparer |> Seq.toList
         observer.Next upserts

      let addToListOfObservablesAndReturnDisposable key1 
                                                    key2 
                                                    (observer: Subject<Upsert<_, _> seq>) 
                                                    (disposable: IDisposable) 
                                                    (observers: List<_ * _ * _>) = 
         observers.Add (key1, key2, observer)
         { new IDisposable with
            member this.Dispose() =
               disposable.Dispose()
               observers.RemoveAll( fun (_, _, o) -> 
                  o = observer
               ) |> ignore
         }


      let createEmits previousKeyValue nextKeyValue : Delta<IKeyable<_>, _> seq = 
           match (previousKeyValue, nextKeyValue) with
           | (None, None) -> Seq.empty
           | (None, Some y) -> seq {  // add (so one emit)
                                       {
                                          Key = fst y
                                          PreviousValue = None
                                          NextValue = Some (snd y)
                                       }
                                   }
           | (Some x, None) -> seq { // delete (so one emit)
                                       {
                                          Key = fst x
                                          PreviousValue = Some (snd x)
                                          NextValue =  None
                                       }
                                   }
           | (Some x, Some y) when (fst x) <> (fst y) -> printfn "whhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaattttttttttttttttttttttttt"; seq {
                                       {
                                          Key = fst x
                                          PreviousValue = Some (snd x)
                                          NextValue =  None
                                       }
                                       {
                                          Key = fst y
                                          PreviousValue = Some (snd y)
                                          NextValue =  None
                                       }
                                    }
           | (Some x, Some y) when (fst x) = (fst y) && (snd x) <> (snd y) -> seq { // if the keys are the same then one emit for updating 
                                                            {
                                                               Key = fst x
                                                               PreviousValue = Some (snd x)
                                                               NextValue =  Some (snd y)
                                                            }
                                                        }
           | (Some x, Some y) -> Seq.empty
      let updateSubscriptions (comparer: Option<IKeyable<'Key> -> IKeyable<'Key> -> int>)
                              (emits: Delta<IKeyable<_>,_> seq)
                              (observers: List<_ * _ * Subject<Upsert<_,_> seq>>) = 
                                 for key1, key2, subject in observers do
                                    let filteredEmits = emits 
                                                         |> Seq.filter (fun delta -> 
                                                                                       match comparer with
                                                                                       | Some comparer -> (comparer delta.Key key1 >= 0) && (comparer delta.Key key2 <= 0)
                                                                                       | None -> (delta.Key.Compare(key1) >= 0) && (delta.Key.Compare(key2) <= 0))
                                                         |> Seq.map (fun delta -> {Key = delta.Key; Value = delta.NextValue})
                                                         |> Seq.toList
                                    if not (Seq.isEmpty filteredEmits) then subject.Next filteredEmits

      let getMappedKeyValue key value (mappingFunction: 'Key -> Option<'Value> -> Option<'NewKey * 'NewValue>) keyExpansionFunction = 
         match mappingFunction key value with
         | Some (newKey, newValue) -> Some (keyExpansionFunction newKey key newValue)
         | None -> None
      let getMappedKeyValueMultiple key value (mappingFunction: 'Key -> 'Value -> seq<'NewKey * 'NewValue>) keyExpansionFunction = 
         let keyValueTuples = mappingFunction key value
         keyValueTuples |> Seq.map (fun (newKey, newValue) -> keyExpansionFunction newKey key newValue ) |> Seq.toList
      let storeNextValueLocally previousKeyValue nextKeyValue (dict: IIndex<_, 'NewValue>) =
         match (previousKeyValue, nextKeyValue) with
         | (None, None) -> () // do nothing
         | (Some x, None) -> dict.WriteBatchPut (fst x) None // delete
         | (None, Some y) -> dict.WriteBatchPut (fst y) (Some (snd y)) // add
         | (Some x, Some y) -> // update and possibly delete if previousKey <> nextKey
                                 if fst x <> fst y then dict.WriteBatchPut (fst x) None
                                 dict.WriteBatchPut (fst y) (Some (snd y))
      let bindKeyOptionValueToOptionKeyValue (f: 'Key  -> 'Value -> Option<'NewKey * 'NewValue>) (key: 'Key) (value: Option<'Value>) : Option<'NewKey * 'NewValue> = 
         match value with
         | Some v -> f key v
         | None -> None
      
      let leftKeyExpansion newKey oldKey value = (({ Key1 = newKey; Key2 = oldKey} :> IKeyable<CompoundKey<_,_>>), value)
      let noKeyExpansion newKey oldKey value = (newKey, value)
      let rightKeyExpansion newKey oldKey value = (({ Key1 = oldKey; Key2 = newKey} :> IKeyable<CompoundKey<_,_>>), value)


   let secondaryIndexNodeStateless 
                  (userSuppliedKeyValueMappingFunction: IKeyable<'Key> -> 'Value -> seq<IKeyable<'NewKey> * 'NewValue>) 
                  (source: IObservable<Delta<IKeyable<'Key>, 'Value> seq * SourceNodePosition>)
                  : IObservable<Delta<IKeyable<CompoundKey<'NewKey, 'Key>>, 'NewValue> seq * SourceNodePosition> = 
      let observable = Subject<Delta<IKeyable<CompoundKey<'NewKey, 'Key>>, 'NewValue> seq * SourceNodePosition>()

      //A helper function so I can get values out of dictionaries as options rather than having to deal with exceptions and nonsense
      let get key (dictionary: IDictionary<_, _>) : Option<'T> = try Some dictionary.[key] with | :? System.Collections.Generic.KeyNotFoundException -> None

      source |> Observable.add (fun (deltas, position) -> 
         try 
            secondaryStopwatch.Start()
            let emits = deltas 
                     |> Seq.map (fun delta -> 
                        let mappedPreviousKeysAndValues = match delta.PreviousValue with
                                                            | Some v -> userSuppliedKeyValueMappingFunction delta.Key v
                                                            | None -> []
                                                            |> Seq.map (fun (newKey, newValue) -> ({Key1 = newKey; Key2 = delta.Key}, newValue) )
                        let mappedNextKeysAndValues = match delta.NextValue with
                                                         | Some v -> userSuppliedKeyValueMappingFunction delta.Key v
                                                         | None -> []
                                                         |> Seq.map (fun (newKey, newValue) -> ({Key1 = newKey; Key2 = delta.Key}, newValue) )

                        let keys = [mappedPreviousKeysAndValues; mappedNextKeysAndValues] |> Seq.concat |> Seq.map (fun (k, v) -> k) |> Seq.distinct
                        let mappedPreviousKeysAndValuesDictionary = mappedPreviousKeysAndValues |> dict
                        let mappedNextKeysAndValuesDictionary = mappedNextKeysAndValues |> dict

                        let emits = 
                           keys 
                              |> Seq.map (fun k ->
                                 HelperFunctions.createEmits 
                                    (mappedPreviousKeysAndValuesDictionary |> get k |> Option.map (fun i -> (k, i))) 
                                    (mappedNextKeysAndValuesDictionary |> get k |> Option.map (fun i -> (k, i))))
                              |> Seq.concat
                        emits
                        ) 
                     |> Seq.concat
            let emits = emits |> Seq.toList
            secondaryStopwatch.Stop()
            if not (Seq.isEmpty emits) then observable.Next (emits, position)
         with e ->
            printfn "%A" e
            raise e
      )
      
      observable

   let primaryIndexNode 
      (wb: WriteBatch)
      (db: RocksDb)
      (rocksDbName: string)
      (comparer: Option<IKeyable<'Key> -> IKeyable<'Key> -> int>)
      (source: IObservable<Upsert<IKeyable<'Key>, 'Value> seq * SourceNodePosition>)
      : PartiallySubscribableSubject<IKeyable<'Key>, 'Value, Delta<IKeyable<'Key>, 'Value> seq * SourceNodePosition> = 
      let index = (new RocksIndex<'Key, 'Value>(db, wb, rocksDbName) :> IIndex<'Key, 'Value>) 
      let observers = new List<IKeyable<'Key> * IKeyable<'Key> * Subject<Upsert<IKeyable<'Key>, 'Value> seq>>()
      let observable = PartiallySubscribableSubject<IKeyable<'Key>, 'Value, Delta<IKeyable<'Key>, 'Value> seq * SourceNodePosition>( fun key1 key2 userSuppliedSubscriptionHandler ->
         let observer = new Subject<Upsert<IKeyable<'Key>, 'Value> seq>()
         let disposable = observer.Subscribe( fun i -> userSuppliedSubscriptionHandler i )
         observer |> HelperFunctions.sendInitialGetToSubscriber key1 
                                                                key2 
                                                                comparer
                                                                //(fun item -> Item {Key = item.Key; Value = item.Value})
                                                                index

         HelperFunctions.addToListOfObservablesAndReturnDisposable key1 key2 observer disposable observers
      , fun key1 key2 snapshot -> 
         let a = match snapshot with
                  | Some s -> index.RangeUsingSnapshot key1 key2 comparer s
                  | None -> index.Range key1 key2 comparer
         a
      )

      source |> Observable.add (fun (upserts, position) -> 
         try
            primaryStopwatch.Start()
            let emits = 
               upserts 
                  |> Seq.map (fun upsert -> 
                        let mappedNextKeyValue = 
                           HelperFunctions.getMappedKeyValue 
                              upsert.Key 
                              upsert.Value
                              (HelperFunctions.bindKeyOptionValueToOptionKeyValue (fun key value -> Some (key, value)))
                              HelperFunctions.noKeyExpansion

                        let mappedPreviousKeyValue = 
                           HelperFunctions.getMappedKeyValue
                              upsert.Key
                              (index.Get upsert.Key)
                              (HelperFunctions.bindKeyOptionValueToOptionKeyValue (fun key value -> Some (key, value)))
                              HelperFunctions.noKeyExpansion


                        HelperFunctions.storeNextValueLocally mappedPreviousKeyValue mappedNextKeyValue index
                        let emits = HelperFunctions.createEmits mappedPreviousKeyValue mappedNextKeyValue
                        emits
                     ) 
                  |> Seq.concat
            let emits = emits |> Seq.toList
            observers |> HelperFunctions.updateSubscriptions comparer emits
            primaryStopwatch.Stop()
            if not (Seq.isEmpty emits) then observable.Next (emits, position)
         with e ->
            printfn "%A" e
            raise e
      )
      
      observable

   let mergeNode 
      (sourcesWithName: (IObservable<Upsert<IKeyable<_>, _> seq * SourceNodePosition> * string) seq)
      : IObservable<Upsert<IKeyable<CompoundKey<_, _>>, 'Value> seq * SourceNodePosition> = 
         let observable = Subject<Upsert<IKeyable<CompoundKey<_, _>>, 'Value> seq * SourceNodePosition>()

         for sourceWithName in sourcesWithName do
            (fst sourceWithName) |> Observable.add (fun upserts ->
               try
                  mergeStopwatch.Start()
                  let emits = (fst upserts) 
                              |> Seq.map (fun upsert ->
                                 let newKey: IKeyable<CompoundKey<_, _>> = {Key1 = upsert.Key; Key2 = {Key = snd sourceWithName}}
                                 let emit = {Key = newKey; Value = upsert.Value}
                                 emit
                              )
                  let emits = emits |> Seq.toList
                  mergeStopwatch.Stop()
                  observable.Next (emits, snd upserts)
               with e ->
                  printfn "%A" e
                  raise e
            )
         
         observable :> IObservable<_>
         
   type LeftOrRight<'a, 'b> = 
   | Lefty of 'a
   | Righty of 'b
   
   let reduceNode
      (wb: WriteBatch)
      (db: RocksDb)
      (rocksDbName: string)
      extractForeignKey 
      provideDefaultValue
      mapLeft
      mapRight
      (source: IObservable<Delta<IKeyable<CompoundKey<'Key, string>>, 'Value> seq * SourceNodePosition>)
      : IObservable<seq<Delta<IKeyable<'Key>, 'NewValue>> * SourceNodePosition>
      = 
         let index = (new RocksIndex<CompoundKey<'Key, _>, 'Value>(db, wb, rocksDbName) :> IIndex<CompoundKey<'Key, _>, 'Value>) 
         let observable = Subject<Delta<IKeyable<'Key>, 'NewValue> seq * SourceNodePosition>()
         let compareFunction = (fun x y -> if x.Key1.Compare(y.Key1) < 0 then -1 else if x.Key1.Compare(y.Key1) > 0 then 1 else 0)


         let matchLeftOrRight value mapFirstCase mapSecondCase = 
            match value with
            | Lefty value -> mapFirstCase value
            | Righty value -> mapSecondCase value

         let matchLeftOrRightAgain value mapFirstCase mapSecondCase = 
            match value with
            | Lefty value -> mapFirstCase value
            | Righty value -> mapSecondCase value

         let createGroupKeyFromDelta = 
            let createGroupKeyFromDelta fieldFromValue matchLeftOrRight (delta: Delta<IKeyable<CompoundKey<_,_>>,_>) = 
               let mapFirstCase originalKey newKey value = Some {Key1 = newKey; Key2 = originalKey}
               let mapSecondCase originalKey fieldFromValue value = Some {Key1 = fieldFromValue value; Key2 = originalKey}
               let createGroupKey fieldFromValue matchLeftOrRight nextValue previousValue newKey originalKey =  
                              let mapFirstCase = mapFirstCase originalKey newKey 
                              let mapSecondCase = mapSecondCase originalKey fieldFromValue
                              match nextValue with
                              | Some value -> matchLeftOrRight value mapFirstCase mapSecondCase
                              | None -> match previousValue with 
                                          | Some value -> matchLeftOrRight value mapFirstCase mapSecondCase
                                          | None -> None
               let key = createGroupKey fieldFromValue matchLeftOrRight delta.NextValue delta.PreviousValue (delta.Key.GetKey().Key1) delta.Key 
               key
            createGroupKeyFromDelta extractForeignKey matchLeftOrRight 


         let mapFunc acc newValue =   
            matchLeftOrRightAgain newValue
               (mapLeft acc)
               (mapRight acc)

         let mapFunc = (fun key value value2 -> 
            let returnValue = 
               (value, value2) ||> Option.map2 mapFunc
            returnValue
         )

         source |> Observable.add (fun (deltas, position) ->
            try
               reduceCounter <- reduceCounter + 1L
               reduceStopwatch.Start()
               let ramIndex = (new RamIndex<CompoundKey<'Key, _>, 'Value>() :> IIndex<CompoundKey<'Key, _>, 'Value>) 
               let emits = deltas |> Seq.map ( fun delta -> 
                  reduceGroupKeyStopwatch.Start()
                  let groupKey = createGroupKeyFromDelta delta
                  reduceGroupKeyStopwatch.Stop()
                  groupKey |> Option.map (fun groupKey ->
                     try
                        reduceGroupKeyStopwatch.Start()
                        let strippedKey = {Key1 = groupKey.Key1; Key2 = {Key1 = groupKey.Key2.GetKey().Key1.LowestPossibleKeyInRange(); Key2 = {Key = ""}}}
                        reduceGroupKeyStopwatch.Stop()
                        reduceRangeStopwatch.Start()
                        let oldUpserts = (index.Range strippedKey strippedKey (Some (fun a b -> if a.GetKey().Key1.Compare(b.GetKey().Key1) < 0 then -1 else if a.GetKey().Key1.Compare(b.GetKey().Key1) > 0 then 1 else 0)))
                        reduceRangeStopwatch.Stop()
                        reduceFoldStopwatch.Start()
                        let oldUpsert = oldUpserts |> Seq.fold (fun upsertAcc upsert -> {Key = upsert.Key.GetKey().Key1; Value = mapFunc (upsert.Key.GetKey().Key1) upsertAcc.Value upsert.Value}) ( {Key = groupKey.Key2.GetKey().Key1.LowestPossibleKeyInRange(); Value = Some provideDefaultValue }) 
                        reduceFoldStopwatch.Stop()
                        reduceWriteBatchStopwatch.Start()
                        index.WriteBatchPut groupKey delta.NextValue
                        reduceWriteBatchStopwatch.Stop()
                        reduceRamindexStopwatch.Start()
                        for x in oldUpserts do ramIndex.Put x.Key x.Value
                        ramIndex.Put groupKey delta.NextValue
                        reduceRamindexStopwatch.Stop()
                        reduceRamindexStopwatch.Start()
                        let newUpserts = (ramIndex.Range strippedKey strippedKey (Some (fun a b -> if a.GetKey().Key1.Compare(b.GetKey().Key1) < 0 then -1 else if a.GetKey().Key1.Compare(b.GetKey().Key1) > 0 then 1 else 0)))
                        reduceRamindexStopwatch.Stop()
                        reduceFoldStopwatch.Start()
                        let newUpsert = newUpserts |> Seq.fold (fun upsertAcc upsert -> {Key = upsert.Key.GetKey().Key1; Value = mapFunc (upsert.Key.GetKey().Key1) upsertAcc.Value upsert.Value}) ( {Key = groupKey.Key2.GetKey().Key1.LowestPossibleKeyInRange(); Value = Some provideDefaultValue }) 
                        reduceFoldStopwatch.Stop()
                        testStopwatch.Start()
                        let emit = {Key = newUpsert.Key; PreviousValue = oldUpsert.Value; NextValue = newUpsert.Value}
                        testStopwatch.Stop()
                        emit
                     with e ->
                        printfn "%A" e
                        raise e
                  )
               )
               let emits = emits |> Seq.choose (fun i -> i)
               let emits = emits |> Seq.toList
               reduceStopwatch.Stop()
               observable.Next (emits, position)
            with e ->
               printfn "%A" e
               raise e
         )
         observable :> IObservable<_>

   let deltaToUpserNode 
      (source: IObservable<Delta<'Key, 'Value> seq * SourceNodePosition>)
      : IObservable<seq<Upsert<'Key, 'Value>> * SourceNodePosition> = 
      let observable = Subject<Upsert<'Key, 'Value> seq * SourceNodePosition>()
      source |> Observable.add (fun (deltas, position) ->
         try
            let emits = 
               deltas 
               |> Seq.map ( fun delta -> {Key = delta.Key; Value = delta.NextValue})
               |> Seq.toList
            observable.Next (emits, position)
         with e ->
            printfn "%A" e
            raise e
      )
      observable
   
   let exportNode
      (kafkaBootstrapServers)
      (outputTopicName)
      (messageParser: Google.Protobuf.MessageParser<'Envelope> when 'Envelope :> Google.Protobuf.IMessage) /// something like Dmg.Providers.V1.ProviderOrg.Parser
      (index: IIndex<UUID, 'Value option * ('Value * SourceNodePosition) option>)
      (positionDict: IIndex<string, int64 * int64>)
      (createEnvelope: SourceNodePosition -> 'Value option -> 'Envelope option when 'Envelope :> Google.Protobuf.IMessage and 'Value :> Google.Protobuf.IMessage)
      (pullPositionAndValueFromEnvelope: 'Envelope -> SourceNodePosition * 'Value when 'Envelope :> Google.Protobuf.IMessage and 'Value :> Google.Protobuf.IMessage)
      (source: IObservable<Delta<IKeyable<UUID>, 'Value> seq * SourceNodePosition> when 'Value :> Google.Protobuf.IMessage)
      : unit = 
      let outputTopic = Kafka.Kafka($"{kafkaBootstrapServers}", outputTopicName)
      let mutable okToProcess = false
////      ////let positionDict = Dictionary<string * int, int64 * int64>()

      async {
         outputTopic.readWholeStream
            (
               fun uuid data offset -> 
                  if data |> Array.isEmpty then
                     index.Put {UUIDKey.Key = uuid} (Some (None, None)) 
                  else
                     let (id, partition, position), value = messageParser.ParseFrom(data) |> pullPositionAndValueFromEnvelope
                     positionDict.Put {StringKey.Key = $"#{id}: #{partition}"} (Some(position, 0L))
////                     positionDict.[(id, partition)] <- (position, 0L)
                     index.Put {UUIDKey.Key = uuid} (Some (Some value, None))
            )
         okToProcess <- true
      } |> Async.Start

      source 
      |> Observable.add (fun (deltas, position) ->
         try
            while not okToProcess do
               async {do! Async.Sleep(100)} |> Async.RunSynchronously
            deltas
            |> Seq.iter (fun delta -> 
               let storedValues = index.Get delta.Key
               index.Put 
                  delta.Key 
                  (
                     match storedValues with
                     | None -> Some (None, delta.NextValue |> Option.map (fun i -> (i, position)))
                     | Some(x) -> Some(fst x, delta.NextValue |> Option.map (fun i -> (i, position)))
                  )
               let id, partition, position = position
               let f = positionDict.Get {StringKey.Key = $"#{id}: #{partition}"}
               match f with
               | None ->
                  positionDict.Put {StringKey.Key = $"#{id}: #{partition}"} (Some(0L, position))
               | Some((lpos, rpos)) -> 
                  positionDict.Put {StringKey.Key = $"#{id}: #{partition}"} (Some(lpos, position))
            )

            if positionDict.GetAllAsSequence() |> Seq.forall(fun kvp -> fst kvp.Value.Value <= snd kvp.Value.Value) then
               index.GetAllAsSequence()
               |> Seq.iter (fun i -> 
                  let (lvalue, rvalueWPosition) = i.Value.Value
                  let rvalue = 
                     match rvalueWPosition with
                     | None -> None
                     | Some(v, p) -> Some(v)
                  let rvaluePos = 
                     match rvalueWPosition with
                     |None -> None
                     |Some(v, p) -> Some(p)
                  if lvalue <> rvalue then
                     async {
                        let optionalBytes = (createEnvelope (rvaluePos |> Option.defaultValue ("", 0 ,0L)) rvalue) |> Option.map Google.Protobuf.MessageExtensions.ToByteArray
                        let! outputTopicWrittenOffset = outputTopic.writeLineToStream(i.Key.GetKey(), optionalBytes |> Option.defaultValue null)
                        ()
                     } |> Async.RunSynchronously
                     index.Put i.Key None
                  else
                     index.Put i.Key None
               )
         with e ->
            printfn "%A" e
            raise e
      )
