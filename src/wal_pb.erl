-file("src/wal_pb.erl", 1).

-module(wal_pb).

-export([encode_checkpoint_rec/1,
	 decode_checkpoint_rec/1,
	 delimited_decode_checkpoint_rec/1, encode_tx_rec/1,
	 decode_tx_rec/1, delimited_decode_tx_rec/1,
	 encode_log_rec/1, decode_log_rec/1,
	 delimited_decode_log_rec/1, encode_log_header/1,
	 decode_log_header/1, delimited_decode_log_header/1]).

-export([has_extension/2, extension_size/1,
	 get_extension/2, set_extension/3]).

-export([decode_extensions/1]).

-export([encode/1, decode/2, delimited_decode/2]).

-record(checkpoint_rec,
	{tstamp, recovery_lsn, last_known_lsn, clock_file}).

-record(tx_rec, {bucket, key, operations}).

-record(log_rec, {lsn, tstamp, tx}).

-record(log_header, {version, start_lsn}).

encode([]) -> [];
encode(Records) when is_list(Records) ->
    delimited_encode(Records);
encode(Record) -> encode(element(1, Record), Record).

encode_checkpoint_rec(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_checkpoint_rec(Record)
    when is_record(Record, checkpoint_rec) ->
    encode(checkpoint_rec, Record).

encode_tx_rec(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_tx_rec(Record) when is_record(Record, tx_rec) ->
    encode(tx_rec, Record).

encode_log_rec(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_log_rec(Record)
    when is_record(Record, log_rec) ->
    encode(log_rec, Record).

encode_log_header(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_log_header(Record)
    when is_record(Record, log_header) ->
    encode(log_header, Record).

encode(log_header, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(log_header, Record) ->
    [iolist(log_header, Record)
     | encode_extensions(Record)];
encode(log_rec, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(log_rec, Record) ->
    [iolist(log_rec, Record) | encode_extensions(Record)];
encode(tx_rec, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(tx_rec, Record) ->
    [iolist(tx_rec, Record) | encode_extensions(Record)];
encode(checkpoint_rec, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(checkpoint_rec, Record) ->
    [iolist(checkpoint_rec, Record)
     | encode_extensions(Record)].

encode_extensions(_) -> [].

delimited_encode(Records) ->
    lists:map(fun (Record) ->
		      IoRec = encode(Record),
		      Size = iolist_size(IoRec),
		      [protobuffs:encode_varint(Size), IoRec]
	      end,
	      Records).

iolist(log_header, Record) ->
    [pack(1, required,
	  with_default(Record#log_header.version, none), uint32,
	  []),
     pack(2, optional,
	  with_default(Record#log_header.start_lsn, none), uint64,
	  [])];
iolist(log_rec, Record) ->
    [pack(1, required,
	  with_default(Record#log_rec.lsn, none), uint64, []),
     pack(2, required,
	  with_default(Record#log_rec.tstamp, none), int64, []),
     pack(3, optional, with_default(Record#log_rec.tx, none),
	  tx_rec, [])];
iolist(tx_rec, Record) ->
    [pack(1, optional,
	  with_default(Record#tx_rec.bucket, none), bytes, []),
     pack(2, required, with_default(Record#tx_rec.key, none),
	  bytes, []),
     pack(3, required,
	  with_default(Record#tx_rec.operations, none), bytes,
	  [])];
iolist(checkpoint_rec, Record) ->
    [pack(1, required,
	  with_default(Record#checkpoint_rec.tstamp, none), int64,
	  []),
     pack(2, required,
	  with_default(Record#checkpoint_rec.recovery_lsn, none),
	  uint64, []),
     pack(3, required,
	  with_default(Record#checkpoint_rec.last_known_lsn,
		       none),
	  uint64, []),
     pack(4, required,
	  with_default(Record#checkpoint_rec.clock_file, none),
	  string, [])].

with_default(Default, Default) -> undefined;
with_default(Val, _) -> Val.

pack(_, optional, undefined, _, _) -> [];
pack(_, repeated, undefined, _, _) -> [];
pack(_, repeated_packed, undefined, _, _) -> [];
pack(_, repeated_packed, [], _, _) -> [];
pack(FNum, required, undefined, Type, _) ->
    exit({error,
	  {required_field_is_undefined, FNum, Type}});
pack(_, repeated, [], _, Acc) -> lists:reverse(Acc);
pack(FNum, repeated, [Head | Tail], Type, Acc) ->
    pack(FNum, repeated, Tail, Type,
	 [pack(FNum, optional, Head, Type, []) | Acc]);
pack(FNum, repeated_packed, Data, Type, _) ->
    protobuffs:encode_packed(FNum, Data, Type);
pack(FNum, _, Data, _, _) when is_tuple(Data) ->
    [RecName | _] = tuple_to_list(Data),
    protobuffs:encode(FNum, encode(RecName, Data), bytes);
pack(FNum, _, Data, Type, _)
    when Type =:= bool;
	 Type =:= int32;
	 Type =:= uint32;
	 Type =:= int64;
	 Type =:= uint64;
	 Type =:= sint32;
	 Type =:= sint64;
	 Type =:= fixed32;
	 Type =:= sfixed32;
	 Type =:= fixed64;
	 Type =:= sfixed64;
	 Type =:= string;
	 Type =:= bytes;
	 Type =:= float;
	 Type =:= double ->
    protobuffs:encode(FNum, Data, Type);
pack(FNum, _, Data, Type, _) when is_atom(Data) ->
    protobuffs:encode(FNum, enum_to_int(Type, Data), enum).

enum_to_int(crdt_type, riak_dt_orswot) -> 1.

int_to_enum(crdt_type, 1) -> riak_dt_orswot;
int_to_enum(_, Val) -> Val.

decode_checkpoint_rec(Bytes) when is_binary(Bytes) ->
    decode(checkpoint_rec, Bytes).

decode_tx_rec(Bytes) when is_binary(Bytes) ->
    decode(tx_rec, Bytes).

decode_log_rec(Bytes) when is_binary(Bytes) ->
    decode(log_rec, Bytes).

decode_log_header(Bytes) when is_binary(Bytes) ->
    decode(log_header, Bytes).

delimited_decode_log_header(Bytes) ->
    delimited_decode(log_header, Bytes).

delimited_decode_log_rec(Bytes) ->
    delimited_decode(log_rec, Bytes).

delimited_decode_tx_rec(Bytes) ->
    delimited_decode(tx_rec, Bytes).

delimited_decode_checkpoint_rec(Bytes) ->
    delimited_decode(checkpoint_rec, Bytes).

delimited_decode(Type, Bytes) when is_binary(Bytes) ->
    delimited_decode(Type, Bytes, []).

delimited_decode(_Type, <<>>, Acc) ->
    {lists:reverse(Acc), <<>>};
delimited_decode(Type, Bytes, Acc) ->
    try protobuffs:decode_varint(Bytes) of
      {Size, Rest} when size(Rest) < Size ->
	  {lists:reverse(Acc), Bytes};
      {Size, Rest} ->
	  <<MessageBytes:Size/binary, Rest2/binary>> = Rest,
	  Message = decode(Type, MessageBytes),
	  delimited_decode(Type, Rest2, [Message | Acc])
    catch
      _What:_Why -> {lists:reverse(Acc), Bytes}
    end.

decode(enummsg_values, 1) -> value1;
decode(log_header, Bytes) when is_binary(Bytes) ->
    Types = [{2, start_lsn, uint64, []},
	     {1, version, uint32, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(log_header, Decoded);
decode(log_rec, Bytes) when is_binary(Bytes) ->
    Types = [{3, tx, tx_rec, [is_record]},
	     {2, tstamp, int64, []}, {1, lsn, uint64, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(log_rec, Decoded);
decode(tx_rec, Bytes) when is_binary(Bytes) ->
    Types = [{3, operations, bytes, []},
	     {2, key, bytes, []}, {1, bucket, bytes, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(tx_rec, Decoded);
decode(checkpoint_rec, Bytes) when is_binary(Bytes) ->
    Types = [{4, clock_file, string, []},
	     {3, last_known_lsn, uint64, []},
	     {2, recovery_lsn, uint64, []}, {1, tstamp, int64, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(checkpoint_rec, Decoded).

decode(<<>>, Types, Acc) ->
    reverse_repeated_fields(Acc, Types);
decode(Bytes, Types, Acc) ->
    {ok, FNum} = protobuffs:next_field_num(Bytes),
    case lists:keyfind(FNum, 1, Types) of
      {FNum, Name, Type, Opts} ->
	  {Value1, Rest1} = case lists:member(is_record, Opts) of
			      true ->
				  {{FNum, V}, R} = protobuffs:decode(Bytes,
								     bytes),
				  RecVal = decode(Type, V),
				  {RecVal, R};
			      false ->
				  case lists:member(repeated_packed, Opts) of
				    true ->
					{{FNum, V}, R} =
					    protobuffs:decode_packed(Bytes,
								     Type),
					{V, R};
				    false ->
					{{FNum, V}, R} =
					    protobuffs:decode(Bytes, Type),
					{unpack_value(V, Type), R}
				  end
			    end,
	  case lists:member(repeated, Opts) of
	    true ->
		case lists:keytake(FNum, 1, Acc) of
		  {value, {FNum, Name, List}, Acc1} ->
		      decode(Rest1, Types,
			     [{FNum, Name, [int_to_enum(Type, Value1) | List]}
			      | Acc1]);
		  false ->
		      decode(Rest1, Types,
			     [{FNum, Name, [int_to_enum(Type, Value1)]} | Acc])
		end;
	    false ->
		decode(Rest1, Types,
		       [{FNum, Name, int_to_enum(Type, Value1)} | Acc])
	  end;
      false ->
	  case lists:keyfind('$extensions', 2, Acc) of
	    {_, _, Dict} ->
		{{FNum, _V}, R} = protobuffs:decode(Bytes, bytes),
		Diff = size(Bytes) - size(R),
		<<V:Diff/binary, _/binary>> = Bytes,
		NewDict = dict:store(FNum, V, Dict),
		NewAcc = lists:keyreplace('$extensions', 2, Acc,
					  {false, '$extensions', NewDict}),
		decode(R, Types, NewAcc);
	    _ ->
		{ok, Skipped} = protobuffs:skip_next_field(Bytes),
		decode(Skipped, Types, Acc)
	  end
    end.

reverse_repeated_fields(FieldList, Types) ->
    [begin
       case lists:keyfind(FNum, 1, Types) of
	 {FNum, Name, _Type, Opts} ->
	     case lists:member(repeated, Opts) of
	       true -> {FNum, Name, lists:reverse(Value)};
	       _ -> Field
	     end;
	 _ -> Field
       end
     end
     || {FNum, Name, Value} = Field <- FieldList].

unpack_value(Binary, string) when is_binary(Binary) ->
    binary_to_list(Binary);
unpack_value(Value, _) -> Value.

to_record(log_header, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       log_header),
						   Record, Name, Val)
			  end,
			  #log_header{}, DecodedTuples),
    Record1;
to_record(log_rec, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields, log_rec),
						   Record, Name, Val)
			  end,
			  #log_rec{}, DecodedTuples),
    Record1;
to_record(tx_rec, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields, tx_rec),
						   Record, Name, Val)
			  end,
			  #tx_rec{}, DecodedTuples),
    Record1;
to_record(checkpoint_rec, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       checkpoint_rec),
						   Record, Name, Val)
			  end,
			  #checkpoint_rec{}, DecodedTuples),
    Record1.

decode_extensions(Record) -> Record.

decode_extensions(_Types, [], Acc) ->
    dict:from_list(Acc);
decode_extensions(Types, [{Fnum, Bytes} | Tail], Acc) ->
    NewAcc = case lists:keyfind(Fnum, 1, Types) of
	       {Fnum, Name, Type, Opts} ->
		   {Value1, Rest1} = case lists:member(is_record, Opts) of
				       true ->
					   {{FNum, V}, R} =
					       protobuffs:decode(Bytes, bytes),
					   RecVal = decode(Type, V),
					   {RecVal, R};
				       false ->
					   case lists:member(repeated_packed,
							     Opts)
					       of
					     true ->
						 {{FNum, V}, R} =
						     protobuffs:decode_packed(Bytes,
									      Type),
						 {V, R};
					     false ->
						 {{FNum, V}, R} =
						     protobuffs:decode(Bytes,
								       Type),
						 {unpack_value(V, Type), R}
					   end
				     end,
		   case lists:member(repeated, Opts) of
		     true ->
			 case lists:keytake(FNum, 1, Acc) of
			   {value, {FNum, Name, List}, Acc1} ->
			       decode(Rest1, Types,
				      [{FNum, Name,
					lists:reverse([int_to_enum(Type, Value1)
						       | lists:reverse(List)])}
				       | Acc1]);
			   false ->
			       decode(Rest1, Types,
				      [{FNum, Name, [int_to_enum(Type, Value1)]}
				       | Acc])
			 end;
		     false ->
			 [{Fnum,
			   {optional, int_to_enum(Type, Value1), Type, Opts}}
			  | Acc]
		   end;
	       false -> [{Fnum, Bytes} | Acc]
	     end,
    decode_extensions(Types, Tail, NewAcc).

set_record_field(Fields, Record, '$extensions',
		 Value) ->
    Decodable = [],
    NewValue = decode_extensions(element(1, Record),
				 Decodable, dict:to_list(Value)),
    Index = list_index('$extensions', Fields),
    erlang:setelement(Index + 1, Record, NewValue);
set_record_field(Fields, Record, Field, Value) ->
    Index = list_index(Field, Fields),
    erlang:setelement(Index + 1, Record, Value).

list_index(Target, List) -> list_index(Target, List, 1).

list_index(Target, [Target | _], Index) -> Index;
list_index(Target, [_ | Tail], Index) ->
    list_index(Target, Tail, Index + 1);
list_index(_, [], _) -> -1.

extension_size(_) -> 0.

has_extension(_Record, _FieldName) -> false.

get_extension(_Record, _FieldName) -> undefined.

set_extension(Record, _, _) -> {error, Record}.

