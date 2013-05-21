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
version() -> "v1.0a".

%% Encodes a number into base 128 varint format.
encodeVarint(Num) when Num < 128 -> <<0:1, Num:7>>;
encodeVarint(Num) ->
	Rem = Num rem 128,
	Res = Num div 128,
	Enc = encodeVarint(Res),
	<<1:1, Rem:7, Enc/binary>>.

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
sendVarint(Socket, Num) ->
	gen_tcp:send(Socket, encodeVarint(Num)).

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

%% Assembles and serializes a close packet into the relay.
sendClose(Socket) ->
	sendByte(Socket, ?OP_CLOSE).

%% Retrieves a single byte from the relay.
recvByte(Socket) ->
	case gen_tcp:recv(Socket, 1) of
		{ok, Packet} -> hd(Packet);
		Error        -> Error
	end.

%% Retrieves a connection initialization response and returns whether ok.
procInit(Socket) ->
	case recvByte(Socket) of
		?OP_INIT                 -> ok;
		Error = {error, _Reason} -> Error;
		_InvalidOpcode           -> {error, violation}
	end.

%% Retrieves messages from the client connection and keeps processing them until
%% either the relay closes (graceful close) or the connection drops.
process(Socket, Handler) ->
	case recvByte(Socket) of
		{error, Reason} -> exit(Reason);
		?OP_CLOSE       -> exit(ok);
		_InvalidOpcode  -> exit(violation)
	end,
	process(Socket, Handler).

