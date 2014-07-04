%% Iris Erlang binding
%% Copyright (c) 2014 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

%% Contains the default service, topic and tunnel limits used by the binding.

%% private

-module(iris_limits).
-export([default_tunnel_buffer/0]).

%% Size of a tunnel's input buffer.
default_tunnel_buffer() -> 64 * 1024 * 1024.
