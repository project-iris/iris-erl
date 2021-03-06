%% Iris Erlang binding
%% Copyright (c) 2013 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

-module(pubsub_tests).
-include_lib("eunit/include/eunit.hrl").
-include("configs.hrl").

-behaviour(iris_server).
-export([init/2, handle_broadcast/2, handle_request/3, handle_tunnel/2,
	handle_drop/2, terminate/2]).


%% Tests multiple concurrent client and service subscriptions and publishes.
publish_test() ->
  % Test specific configurations
	ConfClients = 5,
	ConfServers = 5,
	ConfTopics  = 7,
	ConfEvents  = 15,

	% Pre-generate the topic names
	Topics = [lists:flatten(io_lib:format("~s-~p", [?CONFIG_TOPIC, Index]))
							|| Index <- lists:seq(1, ConfTopics)],

	Barrier = iris_barrier:new(ConfClients + ConfServers),

	% Start up the concurrent publishing clients
	lists:foreach(fun(Client) ->
		spawn(fun() ->
			try
				% Connect to the local relay
				{ok, Conn} = iris_client:start(?CONFIG_RELAY),
				iris_barrier:sync(Barrier),

				% Subscribe to the batch of topics
				lists:foreach(fun(Topic) ->
					ok = iris_client:subscribe(Conn, Topic, pubsub_handler, {self(), Topic})
				end, Topics),
				timer:sleep(100),
				iris_barrier:sync(Barrier),

				% Publish to all subscribers
				lists:foreach(fun(Index) ->
					lists:foreach(fun(Topic) ->
						Event = io_lib:format("client #~p, event ~p", [Client, Index]),
						ok = iris_client:publish(Conn, Topic, list_to_binary(Event))
					end, Topics)
				end, lists:seq(1, ConfEvents)),
				iris_barrier:sync(Barrier),

				% Verify all the topic deliveries
				verify_events(ConfClients, ConfServers, Topics, ConfEvents),
				iris_barrier:sync(Barrier),

        % Unsubscribe from the topics
        lists:foreach(fun(Topic) ->
          ok = iris_client:unsubscribe(Conn, Topic)
        end, Topics),
        iris_barrier:sync(Barrier),

				% Disconnect from the local relay
				ok = iris_client:stop(Conn),
				iris_barrier:exit(Barrier)
      catch
        error:Exception -> iris_barrier:exit(Barrier, Exception)
      end
		end)
	end, lists:seq(1, ConfClients)),

	% Start up the concurrent publishing services
	lists:foreach(fun(Service) ->
		spawn(fun() ->
			try
				% Register a new service to the relay
				{ok, Server} = iris_server:start(?CONFIG_RELAY, ?CONFIG_CLUSTER, ?MODULE, self()),
				Conn = receive
					{ok, Client} -> Client
				end,
				iris_barrier:sync(Barrier),

				% Subscribe to the batch of topics
				lists:foreach(fun(Topic) ->
					ok = iris_client:subscribe(Conn, Topic, pubsub_handler, {self(), Topic})
				end, Topics),
				timer:sleep(100),
				iris_barrier:sync(Barrier),

				% Publish to all subscribers
				lists:foreach(fun(Index) ->
					lists:foreach(fun(Topic) ->
						Event = io_lib:format("server #~p, event ~p", [Service, Index]),
						ok = iris_client:publish(Conn, Topic, list_to_binary(Event))
					end, Topics)
				end, lists:seq(1, ConfEvents)),
				iris_barrier:sync(Barrier),

				% Verify all the topic deliveries
				verify_events(ConfClients, ConfServers, Topics, ConfEvents),
				iris_barrier:sync(Barrier),

        % Unsubscribe from the topics
        lists:foreach(fun(Topic) ->
          ok = iris_client:unsubscribe(Conn, Topic)
        end, Topics),
        iris_barrier:sync(Barrier),

				% Unregister the service
				ok = iris_server:stop(Server),
				iris_barrier:exit(Barrier)
      catch
        error:Exception -> iris_barrier:exit(Barrier, Exception)
      end
		end)
	end, lists:seq(1, ConfServers)),

	% Schedule the parallel operations
	ok = iris_barrier:wait(Barrier),
	ok = iris_barrier:wait(Barrier),
	ok = iris_barrier:wait(Barrier),
  ok = iris_barrier:wait(Barrier),
	ok = iris_barrier:wait(Barrier),
	ok = iris_barrier:wait(Barrier).

%% Verifies the delivered topic events.
verify_events(Clients, Servers, Topics, Events) ->
	% Retrieve all the arrived publishes
	Delivs = ets:new(events, [bag, private]),
	lists:foreach(fun(_) ->
		receive
			{Topic, Binary} -> true = ets:insert(Delivs, {Topic, Binary})
		end
	end, lists:seq(1, (Clients + Servers) * length(Topics) * Events)),

	% Verify all the individual events
	lists:foreach(fun(Client) ->
		lists:foreach(fun(Topic) ->
			lists:foreach(fun(Index) ->
				Event  = io_lib:format("client #~p, event ~p", [Client, Index]),
				Binary = list_to_binary(Event),
				[[]]  = ets:match(Delivs, {Topic, Binary}),
				true   = ets:delete_object(Delivs, {Topic, Binary})
			end, lists:seq(1, Events))
		end, Topics)
	end, lists:seq(1, Clients)),

	lists:foreach(fun(Service) ->
		lists:foreach(fun(Topic) ->
			lists:foreach(fun(Index) ->
				Event  = io_lib:format("server #~p, event ~p", [Service, Index]),
				Binary = list_to_binary(Event),
				[[]]  = ets:match(Delivs, {Topic, Binary}),
				true   = ets:delete_object(Delivs, {Topic, Binary})
			end, lists:seq(1, Events))
		end, Topics)
	end, lists:seq(1, Servers)).


%% Tests the subscription memory limitation.
publish_memory_limit_test() ->
  % Connect to the relay
	{ok, Conn} = iris_client:start(?CONFIG_RELAY),

	% Subscribe to a topic and wait for state propagation
  ok = iris_client:subscribe(Conn, ?CONFIG_TOPIC, pubsub_handler, {self(), ?CONFIG_TOPIC}, [{event_memory, 1}]),
  timer:sleep(100),

  % Check that a 1 byte publish passes
  ok = iris_client:publish(Conn, ?CONFIG_TOPIC, <<0:8>>),
  ok = receive
    _ -> ok
  after
    1 -> timeout
  end,

  % Check that a 2 byte publish is dropped
  ok = iris_client:publish(Conn, ?CONFIG_TOPIC, <<0:8, 1:8>>),
  ok = receive
    _ -> not_dropped
  after
    1 -> ok
  end,

  % Check that space freed gets replenished
  ok = iris_client:publish(Conn, ?CONFIG_TOPIC, <<0:8>>),
  ok = receive
    _ -> ok
  after
    1 -> timeout
  end,

  % Unsubscribe from the topic
  ok = iris_client:unsubscribe(Conn, ?CONFIG_TOPIC),

	% Disconnect from the local relay
	ok = iris_client:stop(Conn).


%% Benchmarks publishing a single message.
publish_latency_benchmark_test() ->
  % Connect to the relay
	{ok, Conn} = iris_client:start(?CONFIG_RELAY),

	% Subscribe to a topic and wait for state propagation
  ok = iris_client:subscribe(Conn, ?CONFIG_TOPIC, pubsub_handler, {self(), ?CONFIG_TOPIC}),
  timer:sleep(100),

  % Benchmark the event transfer latency
  benchmark:iterated(?FUNCTION, 10000, fun() ->
	  ok = iris_client:publish(Conn, ?CONFIG_TOPIC, <<0:8>>),
	  ok = receive
	    _ -> ok
	  after
	    100 -> timeout
	  end
 	end),

  % Unsubscribe from the topic
  ok = iris_client:unsubscribe(Conn, ?CONFIG_TOPIC),

	% Disconnect from the local relay
	ok = iris_client:stop(Conn).


%% Benchmarks publishing a batch of messages.
publish_throughput_1_threads_benchmark_test()   -> ok = publish_throughput_benchmark(1, 50000).
publish_throughput_2_threads_benchmark_test()   -> ok = publish_throughput_benchmark(2, 50000).
publish_throughput_4_threads_benchmark_test()   -> ok = publish_throughput_benchmark(4, 50000).
publish_throughput_8_threads_benchmark_test()   -> ok = publish_throughput_benchmark(8, 50000).
publish_throughput_16_threads_benchmark_test()  -> ok = publish_throughput_benchmark(16, 50000).
publish_throughput_32_threads_benchmark_test()  -> ok = publish_throughput_benchmark(32, 50000).
publish_throughput_64_threads_benchmark_test()  -> ok = publish_throughput_benchmark(64, 50000).
publish_throughput_128_threads_benchmark_test() -> ok = publish_throughput_benchmark(128, 50000).


publish_throughput_benchmark(Threads, Messages) ->
  % Connect to the relay
	{ok, Conn} = iris_client:start(?CONFIG_RELAY),

	% Subscribe to a topic and wait for state propagation
  ok = iris_client:subscribe(Conn, ?CONFIG_TOPIC, pubsub_handler, {self(), ?CONFIG_TOPIC}),
  timer:sleep(100),

  % Create the map and reduce functions
  Map = fun() ->
	  ok = iris_client:publish(Conn, ?CONFIG_TOPIC, <<0:8>>)
	end,
	Reduce = fun() ->
		receive
	    _ -> ok
	  after
	    1000 -> timeout
	  end
	end,

  benchmark:threaded(?FUNCTION, Threads, Messages, Map, Reduce),

  % Unsubscribe from the topic
  ok = iris_client:unsubscribe(Conn, ?CONFIG_TOPIC),

	% Disconnect from the local relay
	ok = iris_client:stop(Conn).


%% =============================================================================
%% Iris server callback methods
%% =============================================================================

%% Simply saves the parent tester for reporting events.
init(Conn, Parent) ->
	Parent ! {ok, Conn},
	{ok, Parent}.

%% No state to clean up.
terminate(_Reason, _State) -> ok.


%% =============================================================================
%% Unused Iris server callback methods (shuts the compiler up)
%% =============================================================================

handle_broadcast(_Message, State) ->
	{stop, unimplemented, State}.

handle_request(_Request, _From, State) ->
	{stop, unimplemented, State}.

handle_tunnel(_Tunnel, State) ->
	{stop, unimplemented, State}.

handle_drop(_Reason, State) ->
	{stop, unimplemented, State}.
