"""Phone-home packet parser.

:Status: $Id: //prod/main/_is/shared/python/phonehome/phlog_entry.py#2 $
:Author: ted, vburenin
"""
from cBencode import bdecode

from shared.phonehome import PHLogConstants as const

class PHPacketFormatError(Exception):

    """Will be generated in case of bad PHLog packet format."""

    pass


class PHLogEntry:

    """A phone-home log entry"""

    def __init__(self, entry):
        """PHLog entry parses.

        In order to handle future message types, be sure to catch the
        PHPacketFormatError thrown when entries of unknown type are parsed.

        :param entry: is the entry returned by PHLogFileReader. It is a tuple
                  containing (timestamp of the entry was written,
                              bencode string).
        """

        self.log_ts = entry[0]
        self.msg_type = None
        self.start_ts = None
        self.end_ts = None
        self.ip_dict = None
        self.all_dict = None
        self.ide_serial = None
        self.ide_interval = None
        self.user_def_gtl = None
        self.auth_str = None
        self.msg_type = None
        self.mga_version = None
        self.av_scanned_files = None
        self.quar_expiration = None
        self.av_neg_attachments = None
        self.av_version = None
        self.post_quar_attachments = None
        self.version = None

        self._parse(entry[1])

    def __unpack_ts(self, packet):
        if packet.has_key(const.GIDX_START_TIME):
            self.start_ts = packet[const.GIDX_START_TIME]
        else:
            raise PHPacketFormatError('The packet does not have start_ts')

        if packet.has_key(const.GIDX_END_TIME):
            self.end_ts = packet[const.GIDX_END_TIME]
        else:
            raise PHPacketFormatError('The packet does not have end_ts')

    def __unpack_ts_int(self, packet):
        try:
            self.start_ts = int(packet[0])
        except Exception:
            raise PHPacketFormatError('The packet does not have start_ts')
        try:
            self.end_ts = int(packet[1])
        except Exception:
            raise PHPacketFormatError('The packet does not have end_ts')

    def __unpack_av_log(self, packet):
        self.__unpack_ts(packet)
        self.attachments = packet.get(const.GIDX_FULL_NAMES, None)

    def __unpack_sbnpv1_per_ip(self, packet):
        self.__unpack_ts_int(packet)

        self.ip_dict = packet[2]

    def __unpack_sbnpv1_global(self, packet):
        self.__unpack_ts(packet)

        self.av_neg_attachments = packet.get(const.GIDX_ATTACHMENTS)
        self.av_scanned_files = packet.get(const.GIDX_FULL_NAMES)
        self.mga_version = packet.get(const.GIDX_MGA_VER)
        self.av_version = packet.get(const.GIDX_SOPHOS_VER)

        try:
            self.ide_serial = int(packet[const.GIDX_IDE_VER])
        except Exception:
            self.ide_serial = None

        try:
            self.ide_interval = int(packet[const.GIDX_IDE_UPDATE])
        except Exception:
            self.ide_interval = None

        try:
            self.user_def_gtl = int(packet[const.GIDX_USER_GTL])
        except Exception:
            self.user_def_gtl = None

        self.post_quar_attachments = packet.get(const.GIDX_POST_QUAR)
        self.quar_reason = packet.get(const.GIDX_QUAR_REASON)
        self.quar_expiration = packet.get(const.GIDX_QUAR_TIME)

    def __unpack_sbnpv2_all(self, packet):
        self.__unpack_ts_int(packet)
        self.all_dict = packet[2]

    def __unpack_user_reg(self, packet):
        """USER_REG packets do not have start and end timestamps."""
        if isinstance(packet, dict):
            self.all_dict = packet
        else:
            raise PHPacketFormatError('The payload must be a dict')

    def _parse(self, bencode_str):
        try:
            e = bdecode(bencode_str)
        except ValueError, e:
            raise PHPacketFormatError('PHLog packet is broken: %s' % (str(e),))
        self.msg_type = e[0]
        self.auth_str = e[1]

        if self.auth_str is None or self.auth_str.strip() == '':
            raise PHPacketFormatError('Authentication string cannot be empty')

        self.version = e[2]

        # Payload starts here.
        packet = e[3]

        if self.msg_type == const.T_AVLOG:
            self.__unpack_av_log(packet)
        elif self.msg_type == const.T_SUBMITSTATS and self.version <= 2:
            self.__unpack_sbnpv1_per_ip(packet)
        elif self.msg_type == const.T_SUBMITGLOBAL and self.version <= 2:
            self.__unpack_sbnpv1_global(packet)
        elif self.msg_type == const.T_SUBMITALL and self.version >= 3:
            self.__unpack_sbnpv2_all(packet)
        elif self.msg_type == const.T_USER_REG and self.version >= 3:
            self.__unpack_user_reg(packet)
        elif self.msg_type == const.T_VTLREQUEST:
            # Empty payload.
            pass
        elif self.msg_type == const.T_VTLRESPONSE:
            # Ignore response msg.
            pass
        elif self.msg_type == const.T_SUBMITSTATSRESP:
            # Ignore response msg.
            pass
        elif self.msg_type == const.T_SUBMITGLOBALRESP:
            # Ignore response msg.
            pass
        else:
            raise PHPacketFormatError('Unknown message type %s of version %s' %
                                      (self.msg_type, self.version))
