"""Common classes and APIs for corpus messages.

:Status: $Id: //prod/main/_is/shared/python/corpus/message.py#4 $
:Authors: jwescott
"""

import os
import time


class CorpusMessageKey(object):
    """This data structure contains everything we need to get a message
    from the database or the file system.  It contains the following
    attributes: category, date, message_id.
    """

    MSG_PFX = "msg"
    """The prefix of the raw corpus messages on disk."""

    RMSG_PFX = "rmsg"
    """The prefix of the rendered messages on disk."""

    def __init__(self, corpus_data_root, add_timestamp, message_id):
        self.corpus_data_root = corpus_data_root
        self.add_timestamp = add_timestamp
        self.message_id = message_id

    def get_path(self):
        ts = time.gmtime(self.add_timestamp)
        timeparts = (self.corpus_data_root,) + tuple(
            time.strftime("%Y/%m/%d/%H", ts).split('/'))
        return os.path.join(*timeparts)

    def get_filename(self):
        msg_file = "%s.%s" % (self.MSG_PFX, self.message_id)
        return os.path.join(self.get_path(), msg_file)

    def get_rendered_filename(self):
        msg_file = "%s.%s" % (self.RMSG_PFX, self.message_id)
        return os.path.join(self.get_path(), msg_file)

    # Properties.
    path = property(get_path)
    filename = property(get_filename)
    rendered_filename = property(get_rendered_filename)
