"""
Solution for the interview task for TWIN
"""


import sys
import os
import json
import glob


class Message:
    """ A message in a dialog """

    def __init__(self, text: str, is_bot: bool):
        self._text = text
        self._is_bot = is_bot

    @property
    def text(self):
        return self._text

    @property
    def is_bot(self):
        return self._is_bot

    def __repr__(self) -> str:
        return "%s: %s" % ("Bot: " if self.is_bot else "Human: ", self.text)


def print_usage():
    print("Usage: python3 twin_bot.py path-to-dialogs-folder")


def main():
    if len(sys.argv) != 2:
        print_usage()
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

    for dialog_filename in dialog_filenames:
        with open(dialog_filename) as dialog_json:
            print("---------------------------------")
            dialog_messages = json.load(dialog_json)
            for msg in dialog_messages:
                message = Message(msg["text"], msg["is_bot"])
                print(message)


if __name__ == "__main__":
    main()
