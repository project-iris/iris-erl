%% Iris Erlang Binding
%% Copyright 2013 Peter Szilagyi. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% decentralized messaging framework, and as such, the same licensing terms
%% hold. For details please see http://github.com/karalabe/iris/LICENSE.md
%%
%% Author: peterke@gmail.com (Peter Szilagyi)

-module(iris_proto).

-export([version/0]).
-compile(export_all).

-define(OP_INIT,     16#00).
-define(OP_BCAST,    16#01).
-define(OP_REQ,      16#02).
-define(OP_REP,      16#03).
-define(OP_SUB,      16#04).
-define(OP_PUB,      16#05).
-define(OP_UNSUB,    16#06).
-define(OP_CLOSE,    16#07).
-define(OP_TUNREQ,   16#07).
-define(OP_TUNREP,   16#08).
-define(OP_TUNACK,   16#0a).
-define(OP_TUNDATA,  16#0b).
-define(OP_TUNPOLL,  16#0c).
-define(OP_TUNCLOSE, 16#0d).

%% Implemented relay protocol version.
version() -> "v1.0".

%% Serializes a single byte into the relay.
sendByte(Socket, Byte) ->
	gen_tcp:send(Socket, [Byte]).

%% Serializes a boolean into the relay.
sendBool(Socket, Bool) ->
	case Bool of
		true  -> sendByte(Socket, 1);
		false -> sendByte(Socket, 0)
	end.

%% Serializes a variable int into the relay.
sendVarint(Socket, Num) when Num < 128 -> sendByte(Socket, Num);
sendVarint(Socket, Num) ->
	Rem = Num rem 128,
	Res = Num div 128,
	case sendByte(Socket, 128 + Rem) of
		ok    -> sendVarint(Socket, Res);
		Error -> Error
	end.

%% Serializes a length-tagged binary array into the relay.
sendBinary(Socket, Binary) ->
	case sendVarint(Socket, byte_size(Binary)) of
		ok    -> gen_tcp:send(Socket, Binary);
		Error -> Error
	end.

%% Serializes a length-tagged string into the relay.
sendString(Socket, String) ->
	sendBinary(Socket, list_to_binary(String)).

%% Assembles and serializes an init packet into the relay.
sendInit(Socket, App) ->
	case sendByte(Socket, ?OP_INIT) of
		ok ->
			case sendString(Socket, version()) of
				ok    -> sendString(Socket, App);
				Error -> Error
			end;
		Error -> Error
	end.

%% Assembles and serializes a broadcast packet into the relay.
sendBroadcast(Socket, App, Message) ->
	case sendByte(Socket, ?OP_BCAST) of
		ok ->
			case sendString(Socket, App) of
				ok    -> sendBinary(Socket, Message);
				Error -> Error
			end;
		Error -> Error
	end.

%% Assembles and serializes a request packet into the relay.
sendRequest(Socket, ReqId, App, Req, Timeout) ->
	case sendByte(Socket, ?OP_REQ) of
		ok ->
			case sendVarint(Socket, ReqId) of
				ok ->
					case sendString(Socket, App) of
						ok ->
							case sendBinary(Socket, Req) of
								ok    -> sendVarint(Socket, Timeout);
								Error -> Error
							end;
						Error -> Error
					end;
				Error -> Error
			end;
		Error -> Error
	end.

%% Assembles and serializes a close packet into the relay.
sendClose(Socket) ->
	sendByte(Socket, ?OP_CLOSE).

%% Retrieves a single byte from the relay.
recvByte(Socket) ->
	case gen_tcp:recv(Socket, 1) of
		{ok, <<Byte:8>>} -> Byte;
		Error            -> Error
	end.

%% Retrieves a boolean from the relay.
recvBool(Socket) ->
	case recvByte(Socket) of
		Error = {error, _Reason} -> Error;
		0 -> false;
		1 -> true;
		_InvalidBool -> {error, violation}
	end.

%% Retrieves a variable int from the relay.
recvVarint(Socket) ->
	case recvByte(Socket) of
		Error = {error, _Reason} -> Error;
		Byte when Byte < 128     -> Byte;
		Byte ->
			case recvVarint(Socket) of
				Error = {error, _Reason} -> Error;
				Num                      -> Byte - 128 + 128 * Num
			end
	end.

%% Retrieves a length-tagged binary array from the relay.
recvBinary(Socket) ->
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		Size ->
			case gen_tcp:recv(Socket, Size) of
				{ok, Binary} -> Binary;
				Error        -> Error
			end
	end.

%% Retrieves a length-tagged binary array from the relay.
recvString(Socket) ->
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		Size ->
			case gen_tcp:recv(Socket, Size) of
				{ok, Binary} -> erlang:bin_to_list(Binary);
				Error        -> Error
			end
	end.

%% Retrieves a connection initialization response and returns whether ok.
procInit(Socket) ->
	case recvByte(Socket) of
		?OP_INIT                 -> ok;
		Error = {error, _Reason} -> Error;
		_InvalidOpcode           -> {error, violation}
	end.

%% Retrieves a remote broadcast message from the relay and notifies the handler.
procBroadcast(Socket, Handler) ->
	case recvBinary(Socket) of
		Error = {error, _Reason} -> Error;
		Msg ->
			Handler ! {broadcast, Msg},
			ok
	end.

%% Retrieves a remote reply to a local request from the relay.
procReply(Socket, Owner) ->
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		ReqId ->
			case recvBool(Socket) of
				Error = {error, _Reason} -> Error;
				true ->
					Owner ! {reply, ReqId, {error, timeout}},
					ok;
				false ->
					case recvBinary(Socket) of
						Error = {error, _Reason} -> Error;
						Reply ->
							Owner ! {reply, ReqId, {ok, Reply}},
							ok
					end
			end
	end.

%% Retrieves messages from the client connection and keeps processing them until
%% either the relay closes (graceful close) or the connection drops.
process(Socket, Owner, Handler) ->
	Res = case recvByte(Socket) of
		Error = {error, _} -> Error;
		?OP_BCAST          -> procBroadcast(Socket, Handler);
		?OP_REP            -> procReply(Socket, Owner);
		?OP_CLOSE          -> exit(ok);
		_InvalidOpcode     -> {error, violation}
	end,
	case Res of
		ok ->
			case process(Socket, Owner, Handler) of
				ok -> ok;
				{error, Reason} -> exit(Reason)
			end;
		{error, Reason} -> exit(Reason)
	end.
