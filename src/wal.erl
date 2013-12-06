-module(wal).
-compile(export_all).

%% Write-ahead log (WAL) library.
%%
%% For maintaining a (roughly) ARIES-style transaction log on disk.

-include("wal_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([create_log/2, open_log/2, close_log/1, log_info/1,
         append/3, read_log_from/3, start_lsn/1, next_lsn/1,
         open_dir/1, init_dir/1, take_checkpoint/2]).
-export([parse_lsn/1, parse_ckpt_path/1]).
-export_type([lsn/0, log_state/0]).

-define(LOG_MAGIC, <<16#BF, 16#9A, 16#02, 16#C7, "CRDT">>).
-define(CKPT_MAGIC, <<16#BF, 16#9A, 16#02, 16#C7, "CKPT">>).
-define(LOG_ALIGN, 64).
-define(LOG_DATA_START, ?LOG_ALIGN).
-define(LOG_HDR_BYTES, 56).
-define(VERSION, 1).

-type lsn() :: non_neg_integer().
-type checkpoint_num() :: non_neg_integer().

-record(log_state, {dir       :: file:filename(),
                    file      :: file:filename(),
                    iodev     :: file:io_device(),
                    mode      :: 'append' | 'read',
                    start_lsn :: lsn(),
                    next_lsn  :: lsn()}).

-type log_state() :: #log_state{}.

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

start_lsn(#log_state{start_lsn=LSN}) when is_integer(LSN) ->
    LSN.
next_lsn(#log_state{next_lsn=LSN}) when is_integer(LSN) ->
    LSN.

open_dir(Dir) ->
    case list_checkpoints(Dir) of
        [] ->
            [] = list_logs(Dir),
            {ok, Log, LSN, Checkpoint} = init_dir(Dir),
            {ready, Log, LSN, Checkpoint};
        CS ->
            {CNum, CPath} = lists:max(CS),
            {checkpoint, CNum, RecoveryLSN} = read_checkpoint(CPath),
            {ok, Logs} = find_logs_for_lsn(Dir, RecoveryLSN),
            {recover, Logs, RecoveryLSN, CNum}
    end.
    

-spec init_dir(file:filename()) ->
                      {'ok', file:filename(), lsn(), checkpoint_num() }.
init_dir(Dir) ->
    [] = list_logs(Dir),
    StartLSN = 0,
    Checkpoint = 0,
    {ok, Log} = create_log(Dir, StartLSN),
    {ok, _CPF} = write_checkpoint(Dir, {checkpoint, Checkpoint, StartLSN}),
    {ok, Log, StartLSN, Checkpoint}.

-spec create_log(file:filename(), non_neg_integer())
                -> {'ok', file:filename()}.

create_log(Dir, StartLSN) ->
    Path = log_path(Dir, StartLSN),
    Header = #log_header{version=?VERSION, start_lsn=StartLSN},
    {ok, HData, ?LOG_HDR_BYTES} = encode_framed(Header, ?LOG_HDR_BYTES),
    ?LOG_DATA_START = byte_size(?LOG_MAGIC) + ?LOG_HDR_BYTES,
    ok = file:write_file(Path, [?LOG_MAGIC, HData], [exclusive]),
    {ok, Path}.

find_logs_for_lsn(Dir, TargetLSN) ->
    {Before, After} =
        lists:partition(fun({LSN, _Path}) -> LSN < TargetLSN end,
                        list_logs(Dir)),
    case lists:sort(After) of
        [] ->
            {_LSN, BPath} = lists:max(Before),
            {ok, [BPath]};
        [{TargetLSN, _Path}|_LS]=AfterS ->
            %% exact match
            {ok, [Path || {_LSN, Path} <- AfterS]};
        AfterS when Before /= [] ->
            {_LSN, BPath} = lists:max(Before),
            {ok, [BPath] ++ [Path || {_LSN_, Path} <- AfterS]}
    end.
            

list_logs(Dir) ->
    list_files(Dir, "wal_*.log", fun parse_lsn/1).

list_checkpoints(Dir) ->
    list_files(Dir, "c_*.ckpt", fun parse_ckpt_path/1).

list_files(Dir, Pattern, Parse) ->
    Files = filelib:wildcard(filename:join(Dir, Pattern)),
    [ Parse(File) || File <- Files ].

parse_lsn(Path) ->
    {ok, [LSN], []} = io_lib:fread("wal_~16u.log", filename:basename(Path)),
    {LSN, Path}.

parse_ckpt_path(Path) ->
    {ok, [CNum], []} = io_lib:fread("c_~16u.ckpt", filename:basename(Path)),
    {CNum, Path}.

-spec take_checkpoint({checkpoint_num(), lsn()}, log_state()) ->
                             {'ok', file:filename()}.
take_checkpoint({Num, LSN}, #log_state{dir=Dir}) ->
    write_checkpoint(Dir, {checkpoint, Num, LSN}).

write_checkpoint(Dir, {checkpoint, Num, LSN}) ->
    Path = checkpoint_path(Dir, Num),
    {error, enoent} = file:read_file_info(Path),
    TmpPath = [Path, ".tmp"],
    {ok, IODev} = file:open(TmpPath, [write, append, binary, raw]),
    ok = file:write(IODev, [?CKPT_MAGIC,
                            term_to_binary({checkpoint, Num, LSN})]),
    ok = file:sync(IODev),
    ok = file:close(IODev),
    ok = file:rename(TmpPath, Path),
    {ok, Path}.

read_checkpoint(Path) ->
    {ok, Bin} = file:read_file(Path),
    Magic = ?CKPT_MAGIC,
    << Magic:8/binary, CRec/binary >> = Bin,
    R={checkpoint, _, _} = binary_to_term(CRec, [safe]),
    R.

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

-spec open_log(file:filename(), 'append' | 'read') -> {'ok', #log_state{}}.

open_log(Path, Mode) ->
    {ok, IODev} = file:open(Path,
                            case Mode of
                                append -> [read, write, append, binary, raw];
                                read   -> [read, binary, raw, read_ahead]
                            end),
    case read_log_header(IODev) of
        {ok, #log_header{start_lsn=StartLSN}} ->
            {ok, EndPos} = file:position(IODev, eof),
            NextLSN = offset_to_lsn(StartLSN, EndPos),
            {ok, #log_state{dir=filename:dirname(Path),
                            file=Path,
                            iodev=IODev,
                            mode=Mode,
                            start_lsn=StartLSN,
                            next_lsn=NextLSN}};
        E={error, _Reason} ->
            E
    end.

-spec close_log(#log_state{}) -> 'ok'.

close_log(#log_state{iodev=IODev}) ->
    ok = file:close(IODev),
    ok.

-spec append(#log_state{mode :: 'append'}, #tx_rec{}, 'sync' | 'nosync')
            -> {'ok', #log_state{mode :: 'append'}}.

append(St=#log_state{mode=append, iodev=IODev, next_lsn=NextLSN},
       TXRec=#tx_rec{}, Sync) when Sync == sync; Sync == nosync ->
    LogRec = build_record(TXRec, NextLSN),
    {ok, Data, RecSize} = encode_framed(LogRec, ?LOG_ALIGN),
    ok = file:write(IODev, Data),
    case Sync of
        sync ->
            ok = file:sync(IODev);
        nosync ->
            ok
    end,
    LSNDelta = RecSize div ?LOG_ALIGN,
    {ok, St#log_state{next_lsn=NextLSN + LSNDelta}}.

-spec read_log_from(#log_state{mode :: 'read'},
                    lsn(),
                    fun((#log_rec{}) -> 'ok' | 'stop'))
                   -> 'ok' | {'error', 'past_end', lsn()}.

read_log_from(LS=#log_state{mode=read, iodev=Dev}, FromLSN, Handler) ->
    StartPos = lsn_to_offset(LS, FromLSN),
    case file:position(Dev, StartPos) of
        {ok, StartPos} ->
            read_log_recs(LS, FromLSN, Handler, <<>>);
        {error, einval} ->
            {error, past_end, FromLSN}
    end.

read_log_recs(LS=#log_state{file=F, mode=read, iodev=Dev},
              LSN, Handler, Buffer) ->
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
                        <<>> ->
                            ok;
                        _Bin ->
                            {ok, Pos} = file:position(Dev, cur),
                            {error, {garbage, F, Pos}}
                    end
            end
    end.

-spec read_log_header(file:io_device())
                     -> {ok, #log_header{start_lsn :: lsn()}}.

read_log_header(Dev) ->
    Magic = ?LOG_MAGIC,
    case file:read(Dev, 64) of
        {ok, <<Magic:8/binary, HData:?LOG_HDR_BYTES/binary>>} ->
            case decode_framed(fun wal_pb:decode_log_header/1,
                               ?LOG_HDR_BYTES, HData) of
                {ok,
                 Header=#log_header{version=?VERSION, start_lsn=StartLSN},
                 ?LOG_HDR_BYTES,
                 <<>>} when is_integer(StartLSN) ->
                    {ok, Header};
                {partial, _} ->
                    {error, corrupt_header}
            end;
        {ok, _} ->
            {error, corrupt_header};
        E={error, _} ->
            E
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
                  io_lib:format("wal_~16.16.0B.log", [StartLSN])).

checkpoint_path(Dir, CNum) ->
    filename:join(Dir,
                  io_lib:format("c_~.16B.ckpt", [CNum])).

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

-spec decode_framed(fun((binary()) -> term()), non_neg_integer(), binary()) ->
                           {'ok', term(), non_neg_integer(), binary()} |
                           {'partial', binary()}.
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
decode_framed(_Decoder, _Align, Partial) when is_binary(Partial) ->
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


%%%% Tests %%%%

-ifdef(TEST).

-define(setup_basic(F), {setup, fun start_basic/0, fun stop_basic/1, F}).

start_basic() ->
    Dir = "/tmp/wal",
    ?assertCmd("sh -c 'if [ -e /tmp/wal ]; then rm -r /tmp/wal; fi'"),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    open_dir(Dir).

stop_basic(_) ->
    ok.

corruption_test_() ->
    [{"Corrupt header magic is detected",
     ?setup_basic(fun mangle_log_header/1)},
     {"Trailing garbage byte is detected",
      ?setup_basic(fun append_stray_byte/1)},
     {"Trailing garbage frame size is detected",
      ?setup_basic(fun append_stray_frame_size/1)}].

mangle_log_header({ready, Log, _, _}) ->
    {ok, IODev} = file:open(Log, [write, binary, raw]),
    ok = file:pwrite(IODev, 0, <<16#FEEDFACE, 16#CAFEBABE>>),
    ok = file:close(IODev),
    Res = wal:open_log(Log, read),
    [?_assertMatch({error, _}, Res)].

append_stray_byte({ready, Log, _, _}) ->
    {ok, IODev} = file:open(Log, [write, binary, append, raw]),
    ok = file:write(IODev, <<16#FC>>),
    ok = file:close(IODev),
    Res = wal:open_log(Log, read),
    [?_assertMatch({error, _}, Res)].

append_stray_frame_size({ready, Log, _, _}) ->
    {ok, IODev} = file:open(Log, [write, binary, append, raw]),
    ok = file:write(IODev, <<0:32>>),
    ok = file:close(IODev),
    Res = wal:open_log(Log, read),
    [?_assertMatch({error, _}, Res)].


-endif.
