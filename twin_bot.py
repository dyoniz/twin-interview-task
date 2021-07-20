"""
Solution for the interview task for TWIN
"""


import sys
import os
import json
import glob
from collections import namedtuple, OrderedDict
import random
import typing as tp

import asyncio
import aiohttp


TWIN_PARSE_URL = "https://sandbox.twin24.ai/parse"
HTTP_STATUS_TOO_MANY_REQUESTS = 429
HTTP_STATUS_OK = 200
MAX_REQUEST_ATTEMPTS = 5


Message = namedtuple("Message", "is_bot, text")


class HttpError(Exception):
    """ Http error """


class IntentParser:
    def __init__(self, parse_api_url: str):
        self._parse_api_url = parse_api_url
        self._intents_by_phrase_cache: dict[str, str] = {}

    async def parse_phrases(self, phrases: tp.Iterable[str]):
        """
        @return: intent by phrase dictionary
        """

        if not phrases:
            return

        new_phrases = []
        for phrase in phrases:
            assert phrase is not None
            phrase = phrase.strip()
            if not phrase:
                continue
            if phrase in self._intents_by_phrase_cache:
                continue
            new_phrases.append(phrase)
        phrases = new_phrases

        parse_phrase_coroutines = []
        for phrase in phrases:
            parse_phrase_coroutines.append(self._parse_phrase_retry(phrase))

        intents = await asyncio.gather(*parse_phrase_coroutines)
        assert len(phrases) == len(intents)
        for phrase, intent in zip(phrases, intents):
            self._intents_by_phrase_cache[phrase] = intent

    def get_intent(self, phrase: str) -> tp.Optional[str]:
        """
        Get intent by phrase or None if phrase was not parsed
        To parse phrases call parse_phrases
        """
        if phrase is None:
            return None
        phrase = phrase.strip()
        if not phrase:
            return None
        return self._intents_by_phrase_cache.get(phrase, None)

    @staticmethod
    def _get_sleep_time(attempt: int):
        assert attempt >= 0
        return attempt / 2 + random.random()

    async def _parse_phrase_retry(self, phrase: str) -> str:
        """
        @return: intent
        """
        assert phrase
        params = {"q": phrase}
        for attempt in range(MAX_REQUEST_ATTEMPTS):
            async with aiohttp.ClientSession() as session:
                async with session.get(self._parse_api_url, params=params) as resp:
                    if resp.status == HTTP_STATUS_OK:
                        resp_json = await resp.json()
                        return resp_json["intent"]["name"]
                    if resp.status == HTTP_STATUS_TOO_MANY_REQUESTS:
                        sleep_sec = IntentParser._get_sleep_time(attempt)
                        print(
                            f"Too many requests, attempt={attempt + 1}, "
                            + "sleeping {round(sleep_sec, 2)} sec"
                        )
                        await asyncio.sleep(sleep_sec)
                        continue
                    raise HttpError(
                        f"Failed to get intent. Server returned {resp.status}"
                    )
        raise TimeoutError(
            f"Failed to get response from server after {MAX_REQUEST_ATTEMPTS} attempts"
        )


class DialogTreeNode:
    """ Dialog tree """

    def __init__(self):
        self.is_bot: bool = True
        self.intent: str = None
        self.phrases: set[str] = set()
        self.replies_by_intent: dict[str, DialogTreeNode] = {}

    def add_reply(self, is_bot: bool, phrase: str, intent: str):
        if is_bot:
            assert intent is None
        else:
            assert intent is not None
        assert phrase

        if intent in self.replies_by_intent:
            next_node = self.replies_by_intent[intent]
            assert next_node.is_bot == is_bot
            assert next_node.intent == intent
        else:
            next_node = DialogTreeNode()
            next_node.is_bot = is_bot
            next_node.intent = intent
            self.replies_by_intent[intent] = next_node

        next_node.phrases.add(phrase)

        return next_node

    def __repr__(self):
        return f"{self.__dict__}"

    @property
    def is_empty(self):
        return not self.phrases


class DialogTree(DialogTreeNode):
    def __init__(self):
        super().__init__()
        self.is_bot = True
        self.intent = None

    def add_dialog(
        self,
        messages: tp.Iterable[Message],
        get_intent: tp.Callable[[str], tp.Optional[str]],
    ):
        if not messages:
            return

        if not messages[0].is_bot:
            messages = messages[1:]
            if not messages:
                return

        assert messages[0].is_bot

        self.phrases.add(messages[0].text)

        next_node = self
        for is_bot, phrase in messages[1:]:
            intent = get_intent(phrase) if not is_bot else None
            assert is_bot or intent is not None, phrase
            next_node = next_node.add_reply(is_bot, phrase, intent)

    @staticmethod
    def encode_json(node: DialogTreeNode):
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
        result["replies"] = list(node.replies_by_intent.values())
        return result


def read_dialog_file(json_filename: str) -> list[Message]:
    """
    Read messages from the dialog file
    @return: list of Message objects (without intent)
    """
    with open(json_filename, encoding="utf8") as dialog_json:
        return [
            Message(is_bot=msg["is_bot"], text=msg["text"])
            for msg in json.load(dialog_json)
        ]


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

    intent_parser = IntentParser(TWIN_PARSE_URL)
    dialog_tree = DialogTree()

    for dialog_filename in dialog_filenames:
        try:
            messages = read_dialog_file(dialog_filename)
            await intent_parser.parse_phrases(
                [text for is_bot, text in messages if not is_bot]
            )
            dialog_tree.add_dialog(messages, intent_parser.get_intent)
        except HttpError as ex:
            print(f"Failed to add dialog to the tree: {dialog_filename}")
            print(ex)
            continue

    print(
        json.dumps(
            dialog_tree, default=DialogTree.encode_json, indent=4, ensure_ascii=False
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
