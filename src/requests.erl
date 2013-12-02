-module(requests).

-define(TABLE, ?MODULE).

-export([init/0, track/2, check/1]).

init() ->
    ets:new(?TABLE, [public, named_table]),
    ok.

track(Request, committed) ->
    true = ets:insert(?TABLE, {Request, committed}),
    ok;

track(Request, acknowledged) ->
    true = ets:delete(?TABLE, Request),
    ok.

check(Request) ->
    case ets:lookup(?TABLE, Request) of
        [] ->
            unknown;
        [{Request, committed}] ->
            committed
    end.
