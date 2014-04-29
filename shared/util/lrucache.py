"""LRUCache.

:Status: $Id: //prod/main/_is/shared/python/util/lrucache.py#4 $
:Author: vburenin
"""

import time


class CacheExpiredError(KeyError):

    """ Used in case requested item has expired. """

    pass


class ANCHOR:

    """Used as an identifier of the end and beginning of linked lists."""

    pass


class _LRUCacheLink(object):

    """Linked list object for the LRU cache."""

    __slots__ = ('left', 'right', 'key', 'obj')


class LRUCache(object):

    """Fast LRU(Least Recently Used) Cache implementation.

      LRU Cache is based on circular linked list that is built on top of
    dictionary to have fast access to any element. 'Anchor' is the beginning
    and the end of linked list. Anchor left element is the oldest one,
    right element is the fresh one.

      Recently accessed/added element is always moved to the top
    of the linked list to track frequency of usage. When cache achieves
    the size limit less used element will be removed from the bottom of the
    linked list and reused as new object.

    LRU Cache tries to reuse existing objects to reduce memory allocation
    overhead.
    """

    __slots__ = ('__size', '__anchor', '__llist')

    def __init__(self, size):
        """Constructor.

        :param size: Static size of LRU cache.
        """
        if size < 2:
            raise ValueError('Cache size can not be less than 2 elements')

        self.__size = size

        # Anchor is the beginning of the linked list.
        self.__anchor = _LRUCacheLink()
        self.__anchor.key = ANCHOR
        self.__anchor.left = self.__anchor
        self.__anchor.right = self.__anchor

        self.__llist = {}

    def clean(self):
        """Clean all cache data."""
        self.__llist.clear()
        self.__anchor.left = self.__anchor
        self.__anchor.right = self.__anchor

    def __len__(self):
        return len(self.__llist)

    def __getitem__(self, key):
        return self.__move_to_top(key).obj

    def __setitem__(self, key, obj):
        """Add new item into cache."""

        if key in self.__llist:
            # Unlink item and put it as fresh element.
            item = self.__move_to_top(key)
            item.obj = obj
        else:
            if len(self.__llist) >= self.__size:
                # Reuse item that is going to be removed.
                new_item = self.__remove_item(self.__anchor.left.key)
            else:
                new_item = _LRUCacheLink()
            new_item.obj = obj
            new_item.key = key
            self.__link_item_as_top(new_item)
            self.__llist[key] = new_item

    def __move_to_top(self, key):
        """Moves element to the top of the list and returns associated item."""
        item = self.__llist[key]
        l_item = item.left
        r_item = item.right
        l_item.right = r_item
        r_item.left = l_item
        self.__link_item_as_top(item)
        return item

    def __link_item_as_top(self, item):
        """Set linked list item to the top of the list."""
        anch = self.__anchor
        anch_r = anch.right
        item.left = anch
        item.right = anch_r
        anch_r.left = item
        anch.right = item

    def __remove_item(self, key):
        """Removes item from the linked list and returns associated item."""
        item = self.__llist.pop(key)
        l_item = item.left
        r_item = item.right
        l_item.right = r_item
        r_item.left = l_item
        return item

    __delitem__ = __remove_item

    def __repr__(self):
        all_items = list()
        curr = self.__anchor.left
        while curr.key != ANCHOR:
            all_items.append((curr.key, curr.obj))
            curr = curr.left
        return repr(all_items)

    def has_key(self, key):
        """Check if cache has the specified element in the storage.

        :param key: Object key.
        :return: True if object exists, otherwise False.
        """
        return key in self.__llist

    def keys(self):
        """Return cached object keys."""
        return self.__llist.keys()

    def dict_copy(self):
        """Make a copy of cache storage.

        :return: Dictionary of cached data.
        """
        res = {}
        for item in self.__llist.itervalues():
            res[item.key] = item.obj
        return res

    def pop(self, key):
        """Remove specified key and return the corresponding value."""
        return self.__remove_item(key).obj

    def get(self, key, default=None):
        """Get data from cache.

        :param key: Object key.
        :param default: Default value if object not found.
        :return: Object associated with key.
        """

        if key in self.__llist:
            return self.__move_to_top(key).obj
        else:
            return default


class LRUTimeCache(object):

    """LRU Cache with objects TTL."""

    def __init__(self, size, ttl):
        """Constructor.

        :param size: Cache size.
        :param ttl: Object cache TTL.
        """
        self.__lru = LRUCache(size)
        self.__ttl = ttl
        self.__hits = 0
        self.__misses = 0
        self.__expired = 0

    def clean(self):
        """Clean cache and reset all stats."""
        self.__lru.clean()
        self.__hits = 0
        self.__misses = 0
        self.__expired = 0

    def stats(self):
        """Returns cache stats.

        :return: tuple(hits, misses, current_cache_size, expired items)
        """
        return (self.__hits, self.__misses, len(self.__lru), self.__expired)

    def put(self, key, obj):
        """Store new object in the cache.

        :param key: Object key.
        :param obj: Object to store.
        """
        self.__lru[key] = (time.time() + self.__ttl, obj)

    def get(self, key):
        """Returns the appropriate object for specified key.

        :param key: Object key.
        :return: Object
        :raise: KeyError if object not found or TTL exceeded.
        """
        try:
            obj = self.__lru[key]
        except KeyError:
            self.__misses += 1
            raise
        else:
            if obj[0] < time.time():
                self.__misses += 1
                self.__expired += 1
                del self.__lru[key]
                raise CacheExpiredError('Key %s has expired.' % (str(key),))

        self.__hits += 1
        return obj[1]

