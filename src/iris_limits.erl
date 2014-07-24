%% Iris Erlang binding
%% Copyright (c) 2014 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

%% Contains the default service, topic and tunnel limits used by the binding.

%% @private

-module(iris_limits).
-export([default_broadcast_memory/0, default_request_memory/0,
  default_topic_memory/0, default_tunnel_buffer/0]).


%% Memory allowance for pending broadcasts.
default_broadcast_memory() -> 64 * 1024 * 1024.

%% Memory allowance for pending requests.
default_request_memory() -> 64 * 1024 * 1024.

%% Memory allowance for pending events.
default_topic_memory() -> 64 * 1024 * 1024.

%% Size of a tunnel's input buffer.
default_tunnel_buffer() -> 64 * 1024 * 1024.
