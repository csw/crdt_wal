-module(dummy_client).

-export([run_client/3, test_parallel/3, passive_runner/2,
         test_client_crdt/1, test_client_crdt/2, test_client_crdt/3]).

%%-record(cstate, {mod, catalog, cat_size, queue}).

-define(CATALOG,
        ordsets:from_list(
          ["Star Wars",
           "Blade Runner",
           "The Muppet Movie",
           "The Mask",
           "Star Trek",
           "Terminator",
           "The Abyss",
           "Alien",
           "Aliens",
           "Days of Heaven"])).

-define(MATRIX,
       [{1,   10},
        {1,   50},
        {1,   100},
        {1,   500},
        {1,   1000},
        {5,   10},
        {5,   20},
        {5,   100},
        {5,   200},
        {10,  1},
        {10,  10},
        {10,  50},
        {10,  100},
        {100, 1},
        {100, 5},
        {100, 10}]).

test_client_crdt(InitF) ->
    io:format("Results for: ~w~n~n", [InitF]),
    io:format("   Clients   Ops/client      Ops    Bytes~n"),
    test_client_crdt(?MATRIX, InitF).

test_client_crdt([], _) ->
    ok;
test_client_crdt([{Clients, Ops}|XS], InitF) ->
    ok = test_client_crdt(Clients, Ops, InitF),
    Size = 0,
    io:format("  ~8w   ~10w ~8w ~8w~n",
              [Clients, Ops, Clients*Ops, Size]),
    test_client_crdt(XS, InitF).


test_client_crdt(Clients, Ops, InitF) ->
    %%{ok, _Server} = crdt_server:start_link(CMod),
    test_parallel(Clients, Ops, InitF),
    %%{ok, _, Size} = crdt_server:info(),
    %%ok = crdt_server:stop(),
    ok.

passive_runner(Key, CatF) ->
    Catalog = read_lines(CatF),
    fun(RID) ->
            PSet = passive_set:fetch(Key, now(), nodes(connected), logged_orswot),
            {ok, {passive_set, PSet, Catalog}}
    end.

test_parallel(Clients, Ops, Init) ->
    Caller = self(),
    spawn_link(fun() ->
                       process_flag(trap_exit, true),
                       start_parallel(Clients, Ops, Init),
                       await_clients(Clients),
                       Caller ! finished
               end),
    receive
        finished -> ok
    end.
    
start_parallel(0, _Ops, _Init) ->
    ok;
start_parallel(C, Ops, Init) ->
    spawn_link(?MODULE, run_client, [Ops, C, Init]),
    start_parallel(C-1, Ops, Init).

await_clients(0) ->
    ok;
await_clients(C) ->
    receive
        {'EXIT', _Pid, _Reason} -> await_clients(C-1)
    end.

run_client(N, C, Init) ->
    ok = util:init_random(),
    {ok, {Mod, StartQueue, CI}} = Init(C),
    client_loop(N, CI, Mod, StartQueue).

client_loop(0, _CI, Mod, Queue) ->
    %% io:format("[~w] final queue contents: ~p~n",
    %%          [self(), Mod:contents(QueueC)]),
    {ok, _NQ} = Mod:sync(Queue),
    {finished};
client_loop(N, CI, Mod, QueueC) ->
    %% io:format("[~w] queue: ~p~n", [self(), QueueC]),
    Queue = Mod:value(QueueC),
    Action = user_action(Queue, CI),
    %% io:format("[~w] action: ~p~n", [self(), Action]),
    NQueue = Mod:update(Action, QueueC),
    NQueue2 = case random:uniform(5) of
                  1 ->
                      {ok, NQueue1} = Mod:sync(NQueue),
                      NQueue1;
                  _ ->
                      NQueue
              end,
    % sleep a bit
    timer:sleep(10 + random:uniform(5)),
    client_loop(N-1, CI, Mod, NQueue2).


%% Simulates a user action.
%%
%% @spec user_action(Queue, Catalog) ->
%%                               {add, Movie} |
%%                               {remove, Movie}
user_action([], Catalog) ->
    %% empty queue, have to add something
    add_some_movie([], Catalog);
user_action(Queue, Catalog) ->
    %% non-empty queue, can add or remove
    case random:uniform(2) of
        1 -> add_some_movie(Queue, Catalog);
        2 -> remove_some_movie(Queue)
    end.

add_some_movie(OldQueue, CatList) ->
    case ordsets:subtract(CatList, OldQueue) of
        [] ->
            %% the entire catalog is already in the queue!
            remove_some_movie(OldQueue);
        Avail ->
            %% choose a random movie from those available
            Movie = lists:nth(random:uniform(ordsets:size(Avail)), Avail),
            {add, Movie}
    end.

remove_some_movie([]) ->
    {error, empty};
remove_some_movie(Queue) ->
    Victim = lists:nth(random:uniform(length(Queue)), Queue),
    {remove, Victim}.
        

read_lines(File) ->
    {ok, Bin} = file:read_file(File),
    binary:split(Bin, <<$\n>>, [global]).
