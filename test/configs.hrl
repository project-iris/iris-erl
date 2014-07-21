%% Iris Erlang binding
%% Copyright (c) 2014 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

-define(CONFIG_RELAY, 55555).
-define(CONFIG_CLUSTER, "erlang-binding-test-cluster").
-define(CONFIG_TOPIC, "erlang-binding-test-topic").

% {current_function, {Module, Function, Arity}} = process_info(self(), current_function)
-define(FUNCTION, element(2, element(2, process_info(self(), current_function)))).
