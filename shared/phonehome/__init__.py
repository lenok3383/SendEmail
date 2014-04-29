"""Dictionary and keys to unpack Phone-home packet.

:Status: $Id: //prod/main/_is/shared/python/phonehome/__init__.py#2 $
:Author: ted, aflury
"""

class PHLogConstants:

    """PHLog data structure constants."""

    # Message types.
    T_AVLOG            = 0 # id of the data generated from avlog by virusCount.
    T_SUBMITSTATS      = 1
    T_VTLREQUEST       = 2
    T_VTLRESPONSE      = 3
    T_SUBMITGLOBAL     = 4
    T_SUBMITSTATSRESP  = 5
    T_SUBMITGLOBALRESP = 6
    T_SUBMITALL        = 7

    T_USER_REG         = 30001

    # Response flags.
    IDX_REQUIRED_VERSION = '0'
    IDX_NEW_SERVER_HOST  = '1'
    IDX_NEW_SERVER_PORT  = '2'
    IDX_NEW_INTERVAL     = '3'

    # Keys for the per-IP stats dictionary.
    IDX_CONNECTIONS = '1'
    IDX_MSG_ACCEPT  = '2'
    IDX_INV_RCPTS   = '3'
    IDX_MSG_BMPOS   = '4'
    IDX_MSG_BMGRAY  = '5'
    IDX_NUM_RCPTS   = '6'
    IDX_TOTAL_SIZE  = '7'
    IDX_ATTACHMENTS = '8'
    IDX_MSG_SAVPOS  = '9'

    IDX_VTS = '10'
    IDX_IPAS_SCORE = '11'
    IDX_MSG_SAV_UNSCANNABLE = '12'
    IDX_MSG_SAV_ENCRYPTED = '13'
    IDX_MSG_BM_NEGATIVE = '14'
    IDX_MSG_BM_TOTAL = '15'
    IDX_MSG_IPAS_NEGATIVE = '16'
    IDX_MSG_IPAS_POSITIVE = '17'
    IDX_MSG_IPAS_SUSPECT = '18'
    IDX_MSG_IPAS_TOTAL = '19'


    # Human-readable descriptions of per-IP counters.
    idx_desc = {
        IDX_CONNECTIONS: 'number of connections',
        IDX_MSG_ACCEPT: 'messages accepted',
        IDX_INV_RCPTS: 'invalid recipients',
        IDX_MSG_BMPOS: 'brightmail positive messages',
        IDX_MSG_BMGRAY: 'brightmail "maybe spam" messages',
        IDX_NUM_RCPTS: 'number of recipients',
        IDX_TOTAL_SIZE: 'aggregate message size',
        IDX_ATTACHMENTS: 'attachment extensions',
        IDX_MSG_SAVPOS: 'sophos positive messages',
    }

    # Keys for sophos verdicts.
    IDX_VERDICT_CLEAN       = '0'
    IDX_VERDICT_VIRAL       = '1'
    IDX_VERDICT_ENCRYPTED   = '2'
    IDX_VERDICT_UNSCANNABLE = '3'

    # Keys for global stats.
    GIDX_START_TIME  = '0'
    GIDX_END_TIME    = '1'
    GIDX_ATTACHMENTS = '2'
    GIDX_FULL_NAMES  = '3'
    GIDX_MGA_VER     = '4'
    GIDX_SOPHOS_VER  = '5'
    GIDX_IDE_VER     = '6'
    GIDX_IDE_UPDATE  = '7'
    GIDX_USER_GTL    = '8'
    GIDX_POST_QUAR   = '9'
    GIDX_QUAR_REASON = '10'
    GIDX_QUAR_TIME   = '11'


    # Avlog content dict.
    AVLOGIDX_MGA_MSGID = '113'
    AVLOGIDX_SOPHOS_TIME = '114'
    AVLOGIDX_VTL_SOURCE = '116'
    AVLOGIDX_FILENAME = '0'
    AVLOGIDX_EXT = '1'
    AVLOGIDX_VERDICT = '2'

    GIDX_DESC = {
        GIDX_START_TIME:  'start time',
        GIDX_END_TIME:    'end time',
        GIDX_ATTACHMENTS: 'attachment extensions',
        GIDX_FULL_NAMES:  'attachment filenames',
        GIDX_MGA_VER:     'MGA version',
        GIDX_SOPHOS_VER:  'Sophos engine version',
        GIDX_IDE_VER:     'Sophos IDE version',
        GIDX_IDE_UPDATE:  'Sophos IDE update interval',
        GIDX_USER_GTL:    'User-configured GTL',
        GIDX_POST_QUAR:   'Post-quarantine attachment extensions',
        GIDX_QUAR_REASON: 'Reason for entering the VOF quarantine',
        GIDX_QUAR_TIME:   'User-configured VOF quarantine retention time',
    }

    # SBNP2 keys.

    AV_CLEAN = 'AV_CLEAN'
    ATTACH_INFO = 'ATTACH_INFO'
    PER_IP_DICT = 'CONNECTION'
    CONNECTION_DICT = 'CONNECTION'
    FILE_INFO = 'FILE_INFO'
    URL_CNT = 'URL'
    SBNPV1_FALL_BACK = 'sbnpv1'
    MGA_VERSION = 'MGA_VERSION'
    QUAR_RETENTION = 'QUAR_RETENTION'
    SOPHOS_ENGINE_VERSION = 'SOPHOS_ENGINE_VERSION'
    SOPHOS_IDE_VERSION = 'SOPHOS_IDE_VERSION'

    # BUG 14139, we will switch from SOPHOS_UPDATE_INTERVAL to UPDATE_INTERVAL.
    SOPHOS_UPDATE_INTERVAL = 'SOPHOS_UPDATE_INTERVAL'
    UPDATE_INTERVAL = 'UPDATE_INTERVAL'

    VTS_THRESHOLD = 'VTS_THRESHOLD'

    IPAS_NEGATIVE_URL = 'IPAS_NEGATIVE_URL'
    IPAS_POSITIVE_URL = 'IPAS_POSITIVE_URL'
    IPAS_UNKNOWN_URL = 'IPAS_UNKNOWN_URL'

    V1_SOPHOS_EXT = 'V1_SOPHOS_EXT'
    V1_SOPHOS_SCANNED = 'V1_SOPHOS_SCANNED'

    AFTER_QUARANTINE = 'AFTER_QUARANTINE'
    POST_QUAR_ATT = 'POST_QUAR_ATT'

    # IPAS RULES.
    IPAS_RULES_COUNT = 'IPAS_RULES_COUNT'

    # Sub dict of V2 Per ip dict.
    CONNECTION_COUNT = 'CONNECTION_COUNT'
    MESSAGE_ACCEPT_COUNT = 'MESSAGE_ACCEPT_COUNT'
    INVALID_RECIPS_COUNT = 'INVALID_RECIPS_COUNT'
    TOTAL_RECIPS_COUNT = 'TOTAL_RECIPS_COUNT'
    BRIGHTMAIL_SPAM_POSITIVE = 'BRIGHTMAIL_SPAM_POSITIVE'
    BRIGHTMAIL_SPAM_SUSPECT = 'BRIGHTMAIL_SPAM_SUSPECT'
    BRIGHTMAIL_SPAM_NEGATIVE = 'BRIGHTMAIL_SPAM_NEGATIVE'
    BRIGHTMAIL_MESSAGE_COUNT = 'BRIGHTMAIL_MESSAGE_COUNT'
    IPAS_SPAM_POSITIVE = 'IPAS_SPAM_POSITIVE'
    IPAS_SPAM_SUSPECT = 'IPAS_SPAM_SUSPECT'
    IPAS_SPAM_NEGATIVE = 'IPAS_SPAM_NEGATIVE'
    IPAS_MESSAGE_COUNT = 'IPAS_MESSAGE_COUNT'
    IPAS_SCORE_LOCAL = 'IPAS_SCORE_LOCAL'
    SOPHOS_CLEAN = 'SOPHOS_CLEAN'
    SOPHOS_VIRAL = 'SOPHOS_VIRAL'
    SOPHOS_UNSCANNABLE = 'SOPHOS_UNSCANNABLE'
    SOPHOS_ENCRYPTED = 'SOPHOS_ENCRYPTED'
    SOPHOS_MESSAGE_COUNT = 'SOPHOS_MESSAGE_COUNT'
    SIZE = 'SIZE'
    LEAVING_QUAR_SOPHOS_RES = 'LEAVING_QUAR_SOPHOS_RES'
    VOF_MESSAGE_COUNT = 'VOF_MESSAGE_COUNT'
    VOF_RULES_COUNT = 'VOF_RULES_COUNT'

    # Global sampling rate according to
    # http://eng.ironport.com/docs/mga/proj/ovation/eng/func_specs/sbnp_changes.rst

    SAMPLING_RATE = 'SAMPLING_RATE'

    # Leaving quarantein rule status, used to replace LEAVING_QUAR_SOPHOS_RES.
    LEAVING_QUAR_RULE = 'LEAVING_QUAR_RULE'
