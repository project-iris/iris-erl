%% Iris Erlang Binding
%% Copyright 2013 Peter Szilagyi. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% decentralized messaging framework, and as such, the same licensing terms
%% hold. For details please see http://github.com/karalabe/iris/LICENSE.md
%%
%% Author: peterke@gmail.com (Peter Szilagyi)

-module(pubsub_tests).
-include_lib("eunit/include/eunit.hrl").

-behaviour(iris_server).
-export([init/1, handle_broadcast/3, handle_request/4, handle_publish/4,
	handle_tunnel/3, handle_drop/2, terminate/2]).

%% Local Iris node's listener port
-define(RELAY_PORT, 55555).


%% =============================================================================
%% Tests
%% =============================================================================

% Subscribes to a handfull of topics, and publishes to each a batch of messages.
single_test() ->
	Events = 75,
	Topics = 75,
	Names = lists:map(fun(_) ->
		base64:encode_to_string(crypto:strong_rand_bytes(64))
	end, lists:seq(1, Topics)),

	% Connect to the Iris network
	{ok, Server, Link} = iris_server:start(?RELAY_PORT, "single", ?MODULE, self()),

	% Subscribe to a few topics
	lists:foreach(fun(Topic) ->
		ok = iris:subscribe(Link, Topic)
	end, Names),

	% Send some random events and check arrival
	Messages = ets:new(events, [set, private]),
	lists:foreach(fun(Topic) ->
		lists:foreach(fun(_) ->
			Event = crypto:strong_rand_bytes(128),
			true = ets:insert_new(Messages, {Event, Topic}),
			ok = iris:publish(Link, Topic, Event)
		end, lists:seq(1, Events))
	end, Names),

	% Retrieve and verify all published events
	lists:foreach(fun(_) ->
		receive
			{Topic, Event} ->
				[{Event, Topic}] = ets:lookup(Messages, Event),
				ets:delete(Messages, Event)
		end
	end, lists:seq(1, Topics * Events)),

	% Close the Iris connection
	ok = iris_server:stop(Server).

% Multiple connections subscribe to the same batch of topics and publish to all.
multi_test() ->
	% Test parameters
	Servers = 10,
	Events = 10,
	Topics = lists:map(fun(_) ->
		base64:encode_to_string(crypto:strong_rand_bytes(64))
	end, lists:seq(1, 10)),

	% Start up the concurrent subscribers (and publishers)
	lists:foreach(fun(_) ->
		Parent = self(),
		spawn(fun() ->
			% Start a single server, subscribe to all the topics and signal parent
			{ok, Server, Link} = iris_server:start(?RELAY_PORT, "multi", ?MODULE, self()),

			lists:foreach(fun(Topic) ->
				ok = iris:subscribe(Link, Topic)
			end, Topics),

			Parent ! {ok, self()},

			% Wait for permission to continue
			receive
				cont -> ok
			end,

			% Publish to the whole group on every topic
			lists:foreach(fun(Topic) ->
				lists:foreach(fun(_) ->
					ok = iris:publish(Link, Topic, list_to_binary(Topic))
				end, lists:seq(1, Events))
			end, Topics),

			% Verify the inbound events
			lists:foreach(fun(Topic) ->
				Event = list_to_binary(Topic),
				lists:foreach(fun(_) ->
					receive
						{Topic, Event} -> ok
					end
				end, lists:seq(1, Events * Servers))
			end, Topics),

			% Terminate the server and signal parent
			ok = iris_server:stop(Server),
			Parent ! done
		end)
	end, lists:seq(1, Servers)),

	% Wait for all the inits
	Pids = lists:map(fun(_) ->
		receive
			{ok, Pid} -> Pid
		end
	end, lists:seq(1, Servers)),

	% Sleep a bit to ensure subscriptions succeeded
	timer:sleep(10),

	% Permit all servers to begin publishing
	lists:foreach(fun(Pid) -> Pid ! cont end, Pids),

	% Wait for all the terminations (big timeout)
	lists:foreach(fun(_) ->
		receive
			done -> ok
		end
	end, lists:seq(1, Servers)).


%% =============================================================================
%% Iris server callback methods
%% =============================================================================

%% Simply saves the parent tester for reporting events.
init(Parent) -> {ok, Parent}.

%% Handles a publish event by reporting it to the tester process.
handle_publish(Topic, Event, Parent, _Link) ->
	Parent ! {Topic, Event},
	{noreply, Parent}.

%% No state to clean up.
terminate(_Reason, _State) -> ok.


%% =============================================================================
%% Unused Iris server callback methods (shuts the compiler up)
%% =============================================================================

handle_broadcast(_Message, State, _Link) ->
	{stop, unimplemented, State}.

handle_request(_Request, _From, State, _Link) ->
	{stop, unimplemented, State}.

handle_tunnel(_Tunnel, State, _Link) ->
	{stop, unimplemented, State}.

handle_drop(_Reason, State) ->
	{stop, unimplemented, State}.
