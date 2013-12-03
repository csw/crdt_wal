-module(wal_mgr).

-include("wal_pb.hrl").

-behaviour(gen_server).

%% API
-export([start_link/0, log_durable/2, clean_record/2]).
-export([take_checkpoint/0, start_req_checkpoint/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(TABLE, outstanding_lsn).

-record(state, {cur_log         :: file:filename(),
                lstate          :: wal:log_state(),
                last_cp         :: wal:checkpoint_num(),
                req_cp_pid=none :: 'none' | pid() }).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec log_durable(crdt_server:crdt_id(), crdt_server:crdt_log_rec()) ->
                         {'ok', wal:lsn()}.
log_durable(Key, Record) ->
    gen_server:call(?SERVER, {log_durable, Key, Record}).

%% For marking log records as clean

-spec clean_record(wal:lsn(), 'data' | 'request') -> 'ok'.
clean_record(LSN, Mode) ->
    true = ets:delete(?TABLE, {LSN, Mode}),
    ok.

take_checkpoint() ->
    gen_server:cast(?SERVER, take_checkpoint).

start_req_checkpoint() ->
    gen_server:cast(?SERVER, start_req_checkpoint).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, Dir} = application:get_env(log_dir),
    _Tab = ets:new(?TABLE, [public, named_table, ordered_set]),
    State =
        case wal:open_dir(Dir) of
            {ready, Log, LSN, Checkpoint} ->
                io:format("Log directory ~s: ready, LSN ~16.16.0B~n",
                          [Dir, LSN]),
                {ok, LS} = wal:open_log(Log, append),
                #state{cur_log=Log, lstate=LS, last_cp={Checkpoint,LSN}};
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
                            io:format("Created new log file: ~s.~n", [NLog]),
                            NLog
                    end,
                {ok, LS} = wal:open_log(Log, append),
                io:format("Recovery complete, next LSN ~16.16.0B~n", [NextLSN]),
                #state{cur_log=Log, lstate=LS, last_cp={LastCP, StartLSN}}
        end,
    ok = crdt_service:finish_recovery(),
    {ok, _RT} =
        timer:apply_interval(timer:minutes(3),
                             gen_server, cast, [self(), start_req_checkpoint]),
    {ok, _CT} =
        timer:apply_interval(timer:minutes(4),
                             gen_server, cast, [self(), take_checkpoint]),
    {ok, State}.

handle_call({log_durable, Key, Record}, _From, S=#state{lstate=LS}) ->
    LSN = wal:next_lsn(LS),
    TxRec = #tx_rec{key=Key, operations=term_to_binary(Record)},
    {Elapsed, {ok, LS1}} = timer:tc(fun() -> wal:append(LS, TxRec) end),
    io:format("Appended log record ~16.16.0B in ~.3f ms~n",
              [LSN, Elapsed/1000]),
    note_record(LSN, Record),
    {reply, {ok, LSN}, S#state{lstate=LS1}}.

handle_cast(take_checkpoint, S0=#state{}) ->
    {ok, S} = take_checkpoint(S0),
    {noreply, S};

handle_cast(start_req_checkpoint,
            S=#state{last_cp={_,LSN}, req_cp_pid=none}) ->
    NPid = start_req_checkpoint(LSN),
    {noreply, S#state{req_cp_pid=NPid}};

handle_cast(start_req_checkpoint,
            S=#state{last_cp={_,LSN}, req_cp_pid=Pid}) when is_pid(Pid) ->
    case is_process_alive(Pid) of
        true ->
            {noreply, S};
        false ->
            NPid = start_req_checkpoint(LSN),
            {noreply, S#state{req_cp_pid=NPid}}
    end.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

take_checkpoint(S=#state{lstate=LS, last_cp={CP, RecLSN}}) ->
    NRecLSN = cur_recovery_lsn(S),
    case NRecLSN > RecLSN of
        true ->
            NextCP = {CP+1, NRecLSN},
            io:format("Taking checkpoint ~B, LSN ~16.16.0B.~n",
                      [CP+1, NRecLSN]),
            {ok, CPFile} = wal:take_checkpoint(NextCP, LS),
            io:format("Checkpoint ~B written as ~s.~n",
                      [CP+1, CPFile]),
            {ok, S#state{last_cp=NextCP}};
        false ->
            io:format("No checkpoint needed.~n"),
            {ok, S}
    end.

cur_recovery_lsn(#state{lstate=LS}) ->
    case ets:first(?TABLE) of
        {LSN,_} ->
            LSN;
        '$end_of_table' ->
            wal:next_lsn(LS)
    end.
                     

start_req_checkpoint(RecoveryLSN) ->    
    {ok, Dir} = application:get_env(data_dir),
    spawn_link(requests, checkpoint, [Dir, RecoveryLSN]).

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

-spec note_record(wal:lsn(), crdt_server:crdt_log_rec()) -> 'ok'.
note_record(LSN, OpRec) ->
    true = ets:insert_new(?TABLE, {{LSN,data}}),
    case crdt_server:is_tracked_request(OpRec) of
        true ->
            true = ets:insert_new(?TABLE, {{LSN,request}});
        false ->
            ok
    end,
    ok.
            

apply_log_record(#log_rec{lsn=LSN, tx=#tx_rec{key=Key, operations=OpsB}}) ->
    {ok, Pid} = crdt_service:find_recovery(Key),
    io:format("LSN ~16.16.0B: CRDT ~p~n", [LSN, Key]),
    Op = binary_to_term(OpsB),
    ok = note_record(LSN, Op),
    ok = crdt_server:recover(Pid, LSN, Op),
    ok.
