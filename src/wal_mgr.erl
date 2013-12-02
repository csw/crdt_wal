-module(wal_mgr).

-include("wal_pb.hrl").

-behaviour(gen_server).

%% API
-export([start_link/0, log_durable/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {cur_log, lstate, last_cp}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

log_durable(Key, Record) ->
    gen_server:call(?SERVER, {log_durable, Key, Record}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, Dir} = application:get_env(log_dir),
    State =
        case wal:open_dir(Dir) of
            {ready, Log, LSN, Checkpoint} ->
                io:format("Log directory ~s: ready, LSN ~16.16.0B~n",
                          [Dir, LSN]),
                {ok, LS} = wal:open_log(Log, append),
                #state{cur_log=Log, lstate=LS, last_cp=Checkpoint};
            {recover, Logs, StartLSN, LastCP} ->
                io:format("Log directory ~s: needs recovery.~n", [Dir]),
                io:format("Starting from LSN ~16.16.0B, checkpoint ~B.~n",
                          [StartLSN, LastCP]),
                {ok, NextLSN} = run_recovery(Logs, StartLSN, seek_ok),
                io:format("Applied all log records.~n"),
                LastLog = lists:last(Logs),
                Log =
                    case wal:parse_lsn(LastLog) of
                        {NextLSN, _} ->
                            LastLog;
                        {LogLSN, _} when LogLSN < NextLSN ->
                            {ok, NLog} = wal:create_log(Dir, NextLSN),
                            io:format("Created new log file: ~s~n.", [NLog]),
                            NLog
                    end,
                {ok, LS} = wal:open_log(Log, append),
                io:format("Recovery complete, next LSN ~16.16.0B~n", [NextLSN]),
                #state{cur_log=Log, lstate=LS, last_cp=LastCP}
        end,
    ok = crdt_service:finish_recovery(),
    {ok, State}.

handle_call({log_durable, Key, Record}, _From, S=#state{lstate=LS}) ->
    LSN = wal:next_lsn(LS),
    {ok, LS1} = wal:append(LS, #tx_rec{key=Key,
                                       operations=term_to_binary(Record)}),
    {reply, {ok, LSN}, S#state{lstate=LS1}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

run_recovery([], StartLSN, _PosMode) ->
    %% All log entries re-applied.
    {ok, StartLSN};
run_recovery([Log|LS], StartLSN, PosMode) ->
    {ok, NextLSN} = recover_from(Log, StartLSN, PosMode),
    run_recovery(LS, NextLSN, beginning).

recover_from(Log, StartLSN, PosMode) ->
    {ok, LS} = wal:open_log(Log, read),
    io:format("Opened log file ~s.~n", [Log]),
    case PosMode of
        beginning ->
            StartLSN = wal:start_lsn(LS);
        seek_ok ->
            ok
    end,
    io:format("Starting LSN: ~16.16.0B~n", [StartLSN]),
    ok = wal:read_log_from(LS, StartLSN, fun apply_log_record/1),
    NextLSN = wal:next_lsn(LS),
    ok = wal:close_log(LS),
    {ok, NextLSN}.

apply_log_record(#log_rec{lsn=LSN, tx=#tx_rec{key=Key, operations=OpsB}}) ->
    {ok, Pid} = crdt_service:find_recovery(Key),
    io:format("LSN ~16.16.0B: CRDT ~w~n", [LSN, Key]),
    Op = binary_to_term(OpsB),
    ok = crdt_server:recover(Pid, LSN, Op),
    ok.
