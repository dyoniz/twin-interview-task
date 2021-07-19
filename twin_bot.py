"""
Solution for the interview task for TWIN
"""


import sys
import os
import json
import glob
from collections import OrderedDict
import asyncio
import aiohttp


TWIN_PARSE_URL = "https://sandbox.twin24.ai/parse"
DEFAULT_INTENT = "default"
HTTP_STATUS_TOO_MANY_REQUESTS = 429
HTTP_STATUS_OK = 200
MAX_ATTEMPTS = 10


class HttpError(Exception):
    """ Http error """


class Message:
    """ A message in a dialog """

    # pylint: disable=too-few-public-methods

    def __init__(self, text: str, is_bot: bool):
        self.text = text
        self.is_bot = is_bot
        self.intent = None

    def __repr__(self) -> str:
        return "%s: %s%s" % (
            "Bot: " if self.is_bot else "Human: ",
            (self.intent + ": ") if not self.is_bot else "",
            self.text,
        )


class DialogTreeNode:
    """ Dialog tree """

    def __init__(self):
        self.is_bot: bool = True
        self.intent: str = None
        self.phrases: set[str] = set()
        # replies by intent
        self.replies: dict[str, DialogTreeNode] = {}

    def add_reply(self, message: Message):
        if message.intent in self.replies:
            next_node = self.replies[message.intent]
            assert next_node.is_bot == message.is_bot
            assert next_node.intent == message.intent
        else:
            next_node = DialogTreeNode()
            next_node.is_bot = message.is_bot
            next_node.intent = message.intent
            self.replies[message.intent] = next_node

        next_node.phrases.add(message.text)

        return next_node

    def __repr__(self):
        return f"{self.__dict__}"

    @property
    def is_empty(self):
        return not self.phrases


def encode_dialog_tree(node: DialogTreeNode):
    if not isinstance(node, DialogTreeNode):
        raise TypeError("%r is not JSON serializable" % (node,))
    if node.is_empty:
        return {}
    assert node.is_bot or node.intent is not None, node
    result = OrderedDict()
    if node.intent is not None:
        result["intent"] = node.intent
    result["is_bot"] = node.is_bot
    result["phrases"] = list(node.phrases)
    result["replies"] = list(node.replies.values())
    return result


def read_messages(dialog_filename: str) -> list[Message]:
    """
    Read messages from the dialog file
    @return: list of Message objects (without intent)
    """
    with open(dialog_filename, encoding="utf8") as dialog_json:
        return [Message(msg["text"], msg["is_bot"]) for msg in json.load(dialog_json)]


async def get_intent(text: str):
    text = text.strip()
    if not text:
        return DEFAULT_INTENT
    params = {"q": text}
    for _ in range(MAX_ATTEMPTS):
        async with aiohttp.ClientSession() as session:
            async with session.get(TWIN_PARSE_URL, params=params) as resp:
                if resp.status == HTTP_STATUS_OK:
                    resp_json = await resp.json()
                    return resp_json["intent"]["name"]
                if resp.status == HTTP_STATUS_TOO_MANY_REQUESTS:
                    await asyncio.sleep(1)
                    continue
                raise HttpError(f"Failed to get intent. Server returned {resp.status}")
    raise TimeoutError("Failed to get intent due to throttling")


async def request_intents(messages: list[Message]):
    # We could request intents in parallel and then use asyncio.gather(),
    # but this would be an overkill for this little task,
    # so we simply requesting intents in a single loop one by one
    for message in messages:
        if not message.is_bot:
            message.intent = await get_intent(message.text)
        else:
            message.intent = None


def update_dialog_tree(node: DialogTreeNode, messages: list[Message]):
    assert node.is_bot
    assert node.intent is None
    if not messages:
        return

    if not messages[0].is_bot:
        messages = messages[1:]
        if not messages:
            return

    assert messages[0].is_bot
    assert messages[0].intent is None

    node.phrases.add(messages[0].text)

    for message in messages[1:]:
        next_node = node.add_reply(message)
        node = next_node


async def main():
    if len(sys.argv) != 2:
        print("Usage: python3 twin_bot.py path-to-dialogs-folder")
        sys.exit(1)

    dialogs_folder = os.path.abspath(sys.argv[1])
    if not os.path.exists(dialogs_folder):
        print(f"Path does not exist: {dialogs_folder}")
        sys.exit(1)

    if not os.path.isdir(dialogs_folder):
        print(f"Specified path is not a folder: {dialogs_folder}")
        sys.exit(1)

    dialog_filenames = glob.glob(os.path.join(dialogs_folder, "*.json"))
    if not dialog_filenames:
        print(f"Couldn't find any json files with dialogs in folder {dialogs_folder}")
        sys.exit(1)

    dialog_tree = DialogTreeNode()

    for dialog_filename in dialog_filenames:
        try:
            messages = read_messages(dialog_filename)
            await request_intents(messages)
            update_dialog_tree(dialog_tree, messages)
        except HttpError as ex:
            print(f"Failed to add dialog to the tree: {dialog_filename}")
            print(ex)
            continue

    print(
        json.dumps(
            dialog_tree, default=encode_dialog_tree, indent=4, ensure_ascii=False
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
