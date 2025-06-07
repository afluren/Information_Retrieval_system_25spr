# from typing import List
# import pickle
# import os
# from tqdm import tqdm


# class TrieNode:
#     def __init__(self):
#         self.children = {}
#         self.is_end = False


# class Trie:
#     def __init__(self):
#         self.root = TrieNode()

#     def insert(self, word: str):
#         node = self.root
#         for char in word:
#             if char not in node.children:
#                 node.children[char] = TrieNode()
#             node = node.children[char]
#         node.is_end = True

#     def search_wildcard(self, pattern: str, reverse=False) -> List[str]:
#         res = []

#         def dfs(node, i, path):
#             if i == len(pattern):
#                 if node.is_end:
#                     if reverse:
#                         res.append("".join(path[::-1]))
#                     else:
#                         res.append("".join(path))
#                 return

#             ch = pattern[i]
#             if ch == '?':
#                 for child in node.children:
#                     dfs(node.children[child], i + 1, path + [child])
#             elif ch == '*':
#                 # '*' 匹配 0 个字符
#                 dfs(node, i + 1, path)
#                 # '*' 匹配 1 个或多个字符
#                 for child in node.children:
#                     dfs(node.children[child], i, path + [child])
#             else:
#                 if ch in node.children:
#                     dfs(node.children[ch], i + 1, path + [ch])

#         dfs(self.root, 0, [])
#         return res


# def create_trie(token_to_id_path: str, reverse=False):
#     trie = Trie()
#     with open(token_to_id_path, "rb") as f:
#         token_to_id = pickle.load(f)
#         for token in tqdm(token_to_id.keys()):
#             if reverse:
#                 token = token[::-1]
#             trie.insert(token)
#     return trie


# if __name__ == "__main__":
#     token_to_id_path = "inverted_index_title/token_to_id.pkl"
#     trie = create_trie(token_to_id_path)
#     reversed_trie = create_trie(token_to_id_path, reverse=True)
#     while True:
#         query = input("Enter query: ")
#         if query == "exit":
#             break
#         if query.startswith("*") or query.startswith("?"):
#             print(reversed_trie.search_wildcard(query[::-1], reverse=True))
#         else:
#             print(trie.search_wildcard(query))
from typing import List
import pickle
import os
from tqdm import tqdm


class Trie:
    def __init__(self, tree=None):
        if tree is None:
            self.tree = {0: {"children": {}, "is_end": False}}
            self.root = 0
            self.length = 1
        else:
            self.tree = tree
            self.root = 0
            self.length = len(tree)

    def insert(self, word: str):
        node = self.tree[self.root]
        for char in word:
            if char not in node["children"]:
                node_id = self.length
                self.tree[node_id] = {"children": {}, "is_end": False}
                node["children"][char] = node_id
                self.length += 1
            node = self.tree[node["children"][char]]
        node["is_end"] = True

    def search_wildcard(self, pattern: str, reverse=False) -> List[str]:
        res = []

        def dfs(node, i, path):
            if i == len(pattern):
                if node["is_end"]:
                    if reverse:
                        res.append("".join(path[::-1]))
                    else:
                        res.append("".join(path))
                return

            ch = pattern[i]
            if ch == '?':
                for child, child_id in node["children"].items():
                    dfs(self.tree[child_id], i + 1, path + [child])
            elif ch == '*':
                # '*' 匹配 0 个字符
                dfs(node, i + 1, path)
                # '*' 匹配 1 个或多个字符
                for child, child_id in node["children"].items():
                    dfs(self.tree[child_id], i, path + [child])
            else:
                if ch in node["children"]:
                    dfs(self.tree[node["children"][ch]], i + 1, path + [ch])

        dfs(self.tree[self.root], 0, [])
        return res


def create_trie(token_to_id_path: str, reverse=False):
    trie = Trie()
    with open(token_to_id_path, "rb") as f:
        token_to_id = pickle.load(f)
        for token in tqdm(token_to_id.keys()):
            if reverse:
                token = token[::-1]
            trie.insert(token)
    return trie


# if __name__ == "__main__":
    # token_to_id_path = "inverted_index_title/token_to_id.pkl"
    # save_path1 = "trie.pkl"
    # save_path2 = "reversed_trie.pkl"
    # trie = create_trie(token_to_id_path)
    # reversed_trie = create_trie(token_to_id_path, reverse=True)
    # with open(save_path1, "wb") as f:
    #     pickle.dump(trie.tree, f)
    # with open(save_path2, "wb") as f:
    #     pickle.dump(reversed_trie.tree, f)

    # save_path1 = "trie.pkl"
    # save_path2 = "reversed_trie.pkl"
    # with open(save_path1, "rb") as f:
    #     trie = pickle.load(f)
    # with open(save_path2, "rb") as f:
    #     reversed_trie = pickle.load(f)
    # while True:
    #     query = input("Enter query: ")
    #     if query == "exit":
    #         break
    #     if query.startswith("*") or query.startswith("?"):
    #         print(reversed_trie.search_wildcard(query[::-1], reverse=True))
    #     else:
    #         print(trie.search_wildcard(query))
