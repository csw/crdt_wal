-module(wal).

%% Write-ahead log (WAL) library.
%%
%% For maintaining a (roughly) ARIES-style transaction log on disk.

-include_lib("kernel/include/file.hrl").
-include("wal_pb.hrl").

-export([create_log/2, open_log/2, close_log/1, log_info/1,
         append/2, read_log_from/3]).

-define(LOG_MAGIC, <<16#BF, 16#9A, 16#02, 16#C7, "CRDT">>).
-define(CKPT_MAGIC, <<16#BF, 16#9A, 16#02, 16#C7, "CKPT">>).
-define(LOG_ALIGN, 64).
-define(LOG_DATA_START, ?LOG_ALIGN).
-define(LOG_HDR_BYTES, 56).
-define(VERSION, 1).

-type lsn() :: non_neg_integer().

-record(log_state, {dir       :: file:name_all(),
                    file      :: file:name_all(),
                    iodev     :: file:io_device(),
                    mode      :: 'append' | 'read',
                    start_lsn :: lsn(),
                    next_lsn  :: lsn()}).

%% Log file layout:
%%     0: magic, 8 bytes
%%     8: log header record (framed)
%%    64: log records (framed, 64-byte aligned)

%% File Header:
%%    0: magic, 8 bytes
%%    8: version, 4 bytes
%%   12: unused
%%   16: starting LSN, 8 bytes

%% Record framing:
%%        0: size, 4 bytes
%%        4: data, size bytes
%%   size+4: CRC32, 4 bytes

%% Checkpoint file:
%%
%% Sidecar file containing a Checkpoint_Rec.
%%
%% 0: magic (8 bytes)
%% 8: Checkpoint_Rec, with record framing


%% Estimate about 57 bytes of overhead for an average log record.
%% With a 64-byte payload: 121 bytes.

%% If the LSN gives the starting offset in 64-byte (2^6 byte)
%% increments

-spec create_log(file:name_all(), non_neg_integer())
                -> {'ok', file:name_all()}.

create_log(Dir, StartLSN) ->
    Path = log_path(Dir, StartLSN),
    Header = #log_header{version=?VERSION, start_lsn=StartLSN},
    {ok, HData, ?LOG_HDR_BYTES} = encode_framed(Header, ?LOG_HDR_BYTES),
    ?LOG_DATA_START = byte_size(?LOG_MAGIC) + ?LOG_HDR_BYTES,
    ok = file:write_file(Path, [?LOG_MAGIC, HData], [exclusive]),
    {ok, Path}.

log_info(Path) ->
    {ok, LS=#log_state{start_lsn=StartLSN,
                       next_lsn=NextLSN}} =
        open_log(Path, read),
    io:format("WAL ~s:~nStarting LSN: ~w~nLog bytes: ~w~nNext LSN:~w~n",
             [Path,
              StartLSN,
              lsn_to_offset(LS, NextLSN)-?LOG_DATA_START,
              NextLSN]),
    ok = close_log(LS),
    ok.

-spec open_log(file:name_all(), 'append' | 'read') -> {'ok', #log_state{}}.

open_log(Path, Mode) ->
    {ok, IODev} = file:open(Path,
                            case Mode of
                                append -> [read, write, append, binary, raw];
                                read   -> [read, binary, raw, read_ahead]
                            end),
    {ok, #log_header{start_lsn=StartLSN}} = read_log_header(IODev),
    {ok, EndPos} = file:position(IODev, eof),
    NextLSN = offset_to_lsn(StartLSN, EndPos),
    {ok, #log_state{dir=filename:dirname(Path),
                    file=Path,
                    iodev=IODev,
                    mode=Mode,
                    start_lsn=StartLSN,
                    next_lsn=NextLSN}}.

-spec close_log(#log_state{}) -> 'ok'.

close_log(#log_state{iodev=IODev}) ->
    ok = file:close(IODev),
    ok.

-spec append(#log_state{mode :: 'append'}, #tx_rec{})
            -> {'ok', #log_state{mode :: 'append'}}.

append(St=#log_state{mode=append, iodev=IODev, next_lsn=NextLSN},
       TXRec=#tx_rec{}) ->
    LogRec = build_record(TXRec, NextLSN),
    {ok, Data, RecSize} = encode_framed(LogRec, ?LOG_ALIGN),
    ok = file:write(IODev, Data),
    LSNDelta = RecSize div ?LOG_ALIGN,
    {ok, St#log_state{next_lsn=NextLSN + LSNDelta}}.

-spec read_log_from(#log_state{mode :: 'read'},
                    lsn(),
                    fun((#log_rec{}) -> 'ok' | 'stop'))
                   -> 'ok'.

read_log_from(LS=#log_state{mode=read, iodev=Dev}, FromLSN, Handler) ->
    StartPos = lsn_to_offset(LS, FromLSN),
    {ok, StartPos} = file:position(Dev, StartPos),
    read_log_recs(LS, FromLSN, Handler, <<>>).

read_log_recs(LS=#log_state{mode=read, iodev=Dev}, LSN, Handler, Buffer) ->
    case decode_framed(fun wal_pb:decode_log_rec/1, ?LOG_ALIGN, Buffer) of
        {ok, Rec=#log_rec{lsn=LSN}, RecSize, Rest} ->
            Handler(Rec),
            read_log_recs(LS, LSN+bytes_to_lsn_delta(RecSize), Handler, Rest);
        {partial, Buffer} ->
            case file:read(Dev, 65536) of
                {ok, Buf2} when is_binary(Buf2) ->
                    read_log_recs(LS, LSN, Handler,
                                  <<Buffer/binary, Buf2/binary>>);
                eof ->
                    case Buffer of
                        <<>> -> ok
                    end
            end
    end.

-spec read_log_header(file:io_device())
                     -> {ok, #log_header{start_lsn :: lsn()}}.

read_log_header(Dev) ->
    Magic = ?LOG_MAGIC,
    {ok, <<Magic:8/binary, HData:?LOG_HDR_BYTES/binary>>} = file:read(Dev, 64),
    {ok, Header=#log_header{version=?VERSION, start_lsn=StartLSN},
         ?LOG_HDR_BYTES,
         <<>>} =
        decode_framed(fun wal_pb:decode_log_header/1,
                      ?LOG_HDR_BYTES, HData),
    case is_integer(StartLSN) of
        true -> {ok, Header}
    end.

bytes_to_lsn_delta(Bytes) when Bytes rem ?LOG_ALIGN == 0 ->
    Bytes div ?LOG_ALIGN.

-spec offset_to_lsn(lsn(), non_neg_integer()) -> lsn().

offset_to_lsn(StartLSN, Offset) ->
    bytes_to_lsn_delta(Offset - ?LOG_DATA_START) + StartLSN.

-spec lsn_to_offset(#log_state{} | lsn(), lsn()) -> non_neg_integer().

lsn_to_offset(#log_state{start_lsn=StartLSN}, LSN) ->
    lsn_to_offset(StartLSN, LSN);
lsn_to_offset(StartLSN, LSN) ->
    ?LOG_DATA_START + (LSN-StartLSN)*?LOG_ALIGN.

log_path(Dir, StartLSN) ->
    filename:join(Dir,
                  io_lib:format("wal_~w.log", [StartLSN])).

build_record(TX=#tx_rec{}, LSN) ->
    #log_rec{lsn=LSN,
             tstamp=unix_timestamp(),
             tx=TX}.

encode_framed(Record, PadAlign) ->
    RBin = iolist_to_binary(wal_pb:encode(Record)),
    CRC = erlang:crc32(RBin),
    RSize = byte_size(RBin),
    Padding = padding_to(RSize+8, PadAlign),
    Data = [<<RSize:32>>, RBin, <<CRC:32>>, Padding],
    TotalSize = RSize+8+byte_size(Padding),
    0 = TotalSize rem PadAlign,
    {ok, Data, TotalSize}.

decode_framed(Decoder, Align,
              <<RSize:32, RBin:RSize/binary, CRC:32,
                Trailing/binary>>) when is_integer(Align) ->
    PadBytes = padding_bytes(RSize+8, Align),
    case Trailing of
        <<_Padding:PadBytes/unit:8, Rest/binary>> ->
            %% correctly padded
            CRC = erlang:crc32(RBin),
            {ok, Decoder(RBin), RSize+8+PadBytes, Rest};
        _ ->
            {partial, Trailing}
    end;
decode_framed(_Decoder, _Align, Partial)
  when is_binary(Partial), byte_size(Partial) > 0 ->
    {partial, Partial}.


%% PKCS#7 style padding
padding_to(Size, Align) ->
    case padding_bytes(Size, Align) of
        0 -> <<>>;
        N -> binary:copy(<<N:1/unit:8>>, N)
    end.

padding_bytes(0, _Align) ->
    0;
padding_bytes(Size, Align) when Size < Align ->
    Align - Size;
padding_bytes(Size, Align) ->
    padding_bytes(Size rem Align, Align).



unix_timestamp() ->
    {MegaSecs, Secs, _} = os:timestamp(),
    MegaSecs * 1000000 + Secs.
