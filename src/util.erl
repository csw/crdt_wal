-module(util).

-export([init_random/0]).

init_random() ->
    <<A:32, B:32, C:32>> = crypto:strong_rand_bytes(12),
    random:seed(A, B, C),
    ok.
