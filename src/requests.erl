-module(requests).
-include_lib("stdlib/include/ms_transform.hrl").

-define(TABLE, ?MODULE).
-define(TABLE_FILE, "requests.ets").

-export([init/1, committed/2, acknowledge/1, check/1, checkpoint/2]).

init(Dir) ->
    File = filename:join(Dir, ?TABLE_FILE),
    case filelib:is_regular(File) of
        true ->
            {ok, Info} = ets:tabfile_info(File),
            {name, ?TABLE} = lists:keyfind(name, 1, Info),
            {type, set} = lists:keyfind(type, 1, Info),
            {named_table, true} = lists:keyfind(named_table, 1, Info),
            {protection, public} = lists:keyfind(protection, 1, Info),
            {ok, _Tab} = ets:file2tab(File, [{verify, true}]),
            ok;
        false ->
            ets:new(?TABLE, [public, named_table]),
            ok
    end.

-spec committed(crdt_service:request_id(), wal:lsn()) -> 'ok'.
committed(Request, LSN) ->
    true = ets:insert(?TABLE, {Request, LSN, committed}),
    ok.


-spec acknowledge(crdt_service:request_id()) -> 'ok'.
acknowledge(Request) ->
    case ets:lookup(?TABLE, Request) of
        [{Request, _LSN, committed}] ->
            %% could just delete it if we had the checkpoint LSN
            %%ets:insert(?TABLE, {Request, LSN, acknowledged});
            true = ets:update_element(?TABLE, Request, {3,acknowledged}),
            ok;
        [{_Request, _LSN, acknowledged}] ->
            ok;
        [] ->
            ok
    end.

-spec check(crdt_service:request_id()) -> 'committed' |
                                          'acknowledged' |
                                          'unknown'.
check(Request) ->
    case ets:lookup(?TABLE, Request) of
        [] ->
            unknown;
        [{Request, _LSN, State}] ->
            State
    end.

%% Once a request is in the ETS table, it is present for future requests.
%%
%% Once the ETS table has been synced to disk, any LSNs in the
%% serialized version are written out for request tracking purposes.
%%
%% When taking inventory, we only need to find LSNs starting at the
%% last checkpoint's recovery LSN.
%%
%% We also want to mark as clean any log records for requests that
%% have been acked before the checkpoint.

-spec checkpoint(file:filename(), wal:lsn()) -> 'ok'.
checkpoint(Dir, RecoveryLSN) ->
    File = filename:join(Dir, ?TABLE_FILE),
    Temp = filename:join(Dir, ?TABLE_FILE ++ ".tmp"),
    io:format("Beginning request table checkpoint, recovery LSN ~16.16.0B.~n",
              [RecoveryLSN]),
    case gc_scan(RecoveryLSN) of
        {ok, 0} ->
            ok;
        {ok, Num} ->
            io:format("Removed ~B old requests.~n", [Num])
    end,
    MS = ets:fun2ms(fun({Req, LSN, _}) when LSN >= RecoveryLSN -> LSN end),
    Fresh = ets:select(?TABLE, MS),
    ok = ets:tab2file(?TABLE, Temp, [{extended_info, [object_count]}]),
    ok = file:rename(Temp, File),
    {ok, Info} = ets:tabfile_info(File),
    {size, Reqs} = lists:keyfind(size, 1, Info),
    io:format("Wrote table of ~B requests to ~s.~n",
              [Reqs, File]),
    NFresh = lists:foldl(fun(LSN, N) ->
                                 ok = wal_mgr:clean_record(LSN, request),
                                 N+1
                         end,
                         0,
                         Fresh),
    io:format("Recorded ~B fresh requests since last checkpoint.~n",
              [NFresh]),
    ok.

gc_scan(RecoveryLSN) ->
    MS = ets:fun2ms(fun({_, LSN, acknowledged})
                          when LSN < RecoveryLSN ->
                            true
                    end),
    Num = ets:select_delete(?TABLE, MS),
    {ok, Num}.
