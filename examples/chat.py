"""CometD Chat Example Client."""

import argparse
import asyncio
from contextlib import suppress
from typing import Any, Dict

from aioconsole import ainput  # type: ignore

from aiocometd import Client, ConnectionType
from aiocometd.exceptions import AiocometdException


async def chat_client(url: str, nickname: str, connection_type: ConnectionType) -> None:
    """Run a CometD chat client until it is canceled.

    Args:
        url (str): The CometD server URL.
        nickname (str): The user's nickname.
        connection_type (ConnectionType): The connection transport type.
    """
    room_name = "demo"
    room_channel = f"/chat/{room_name}"
    members_changed_channel = f"/members/{room_name}"
    members_channel = "/service/members"

    try:
        async with Client(url, connection_type) as client:
            print(f"Connected to '{url}' using '{connection_type.value}' transport.\n")

            # Subscribe to channels
            await client.subscribe(room_channel)
            await client.subscribe(members_changed_channel)

            # Announce presence
            await client.publish(room_channel, {
                "user": nickname,
                "membership": "join",
                "chat": f"{nickname} has joined"
            })

            # Add user to the members list
            await client.publish(members_channel, {
                "user": nickname,
                "room": room_channel
            })

            # Start background task for user input
            input_task = asyncio.create_task(input_publisher(client, nickname, room_channel))
            last_user = None

            try:
                async for message in client:
                    channel = message["channel"]
                    data = message.get("data", {})

                    if channel == room_channel:
                        user = data["user"]
                        prefix = "..." if user == last_user else f"{user}:"
                        last_user = user
                        print(f"{prefix} {data['chat']}")

                    elif channel == members_changed_channel:
                        print("MEMBERS:", ", ".join(data))
                        last_user = None

            finally:
                input_task.cancel()
                with suppress(asyncio.CancelledError):
                    await input_task

    except AiocometdException as error:
        print(f"Encountered an error: {error}")
    except asyncio.CancelledError:
        pass
    finally:
        print("\nExiting...")


async def input_publisher(client: Client, nickname: str, room_channel: str) -> None:
    """Read user input from stdin and publish it to the chat room.

    Args:
        client (Client): The active CometD client.
        nickname (str): The user's nickname.
        room_channel (str): The chat room channel name.
    """
    up_one_line = "\033[F"
    clear_line = "\033[K"

    while True:
        try:
            message_text = await ainput("")
        except asyncio.CancelledError:
            break

        # Clear previous input line
        print(up_one_line, end="")
        print(clear_line, end="", flush=True)

        # Publish the user's message
        await client.publish(room_channel, {
            "user": nickname,
            "chat": message_text
        })


def get_arguments() -> Dict[str, Any]:
    """Parse command-line arguments for the chat client.

    Returns:
        Dict[str, Any]: Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(description="CometD chat example client")
    parser.add_argument("url", metavar="server_url", type=str, help="CometD server URL")
    parser.add_argument("nickname", type=str, help="Chat nickname")
    parser.add_argument(
        "-c",
        "--connection_type",
        type=ConnectionType,
        choices=list(ConnectionType),
        default=ConnectionType.WEBSOCKET,
        help="Connection type (default: WEBSOCKET)",
    )
    return vars(parser.parse_args())


def main() -> None:
    """Start the CometD chat client application."""
    arguments = get_arguments()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    chat_task = loop.create_task(chat_client(**arguments))

    try:
        loop.run_until_complete(chat_task)
    except KeyboardInterrupt:
        chat_task.cancel()
        with suppress(asyncio.CancelledError):
            loop.run_until_complete(chat_task)
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


if __name__ == "__main__":
    main()
