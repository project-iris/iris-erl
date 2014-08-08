%% Iris Erlang binding
%% Copyright (c) 2014 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

-module(pubsub_handler).

-behaviour(iris_topic).
-export([init/1, handle_event/2, terminate/2]).


%% Simply saves the parent tester for reporting events.
init(State = {_Parent, _Topic}) -> {ok, State}.

%% Handles a topic event by reporting it to the tester process.
handle_event(Event, State = {Parent, Topic}) ->
	Parent ! {Topic, Event},
	{noreply, State}.

%% No state to clean up.
terminate(_Reason, _State) -> ok.
