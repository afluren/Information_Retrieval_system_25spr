# TODO：这里的向量空间模型不完全，未来可能会加除以向量长度的操作
from inverted_index import tokenize as parse_query
from wildcard_query import Trie
from typing import List, Dict
import sqlite3
import pickle
import os
import math

# 这里接受的tokens会过滤停用词
TOTAL_NUM = 266300
MAX_PHRASE_LEN = 3
TITLE_WEIGHT = 3.0
TEXT_WEIGHT = 1.0
MAX_TOKENS_LEN = 100
with open("inverted_index/doc_info.pkl", "rb") as f:
    doc_info = pickle.load(f)
with open("inverted_index_title/token_to_id.pkl", "rb") as f:
    token_to_id = pickle.load(f)
with open("trie.pkl", "rb") as f:
    trie = pickle.load(f)
with open("reversed_trie.pkl", "rb") as f:
    reversed_trie = pickle.load(f)


def get_freq_from_text(query_tokens: List[str]) -> Dict:
    tokens_info = {}

    conn = sqlite3.connect("inverted_index.db")
    cursor = conn.cursor()

    # TODO: 这里应该解决词项不存在的情况
    for token in query_tokens:
        if token in token_to_id:
            token_id = token_to_id[token]
            cursor.execute(
                "SELECT doc_id, freq FROM inverted_index WHERE token=?", (token_id,))
            rows = cursor.fetchall()
            df = len(rows)
            tokens_info[token] = {int(doc_id): int(freq)
                                  for doc_id, freq in rows}
            tokens_info[token]['df'] = df
        else:
            continue
    conn.close()
    return tokens_info


def get_freq_from_title(query_tokens: List[str]) -> Dict:
    tokens_info = {}

    conn = sqlite3.connect("inverted_index.db")
    cursor = conn.cursor()

    # TODO: 这里应该解决词项不存在的情况
    for token in query_tokens:
        if token in token_to_id:
            token_id = token_to_id[token]
            cursor.execute(
                "SELECT doc_id, freq FROM title_index WHERE token=?", (token_id,))
            rows = cursor.fetchall()
            df = len(rows)
            tokens_info[token] = {int(doc_id): int(freq)
                                  for doc_id, freq in rows}
            tokens_info[token]['df'] = df
        else:
            continue
    conn.close()
    return tokens_info


def Total_TF_IDF(doc_id: int, query_tokens_info: Dict, type: str = "text") -> float:
    """
    计算TF-IDF值
    :param doc_id: 文档ID
    :param tokens_info: 词项信息
    :param type: 计算TF-IDF的类型，text表示计算文本的TF-IDF值，title表示计算标题的TF-IDF值
    :return: TF-IDF值
    """
    score = 0.0
    for query_token in query_tokens_info:
        query_token_info = query_tokens_info[query_token]
        if doc_id in query_token_info:
            if type == "text":
                doc_length = doc_info[doc_id]['length']
            elif type == "title":
                doc_length = len(parse_query(doc_info[doc_id]['title']))
            tf = query_token_info[doc_id] / doc_length
            df = query_token_info['df']
            idf = math.log(TOTAL_NUM / (df + 1))
            score += tf * idf
    return score

# 常规站内查询，直接对查询进行分词，然后计算TF-IDF值


def text_query(query_string: str):
    query_tokens = parse_query(query_string)
    tokens_info_text = get_freq_from_text(query_tokens)
    tokens_info_title = get_freq_from_title(query_tokens)
    doc_set = set()
    for token in tokens_info_text:
        doc_set.update(tokens_info_text[token].keys())
    for token in tokens_info_title:
        doc_set.update(tokens_info_title[token].keys())
    doc_set.discard('df')  # 移除df键
    scores = {}
    for doc_id in doc_set:
        scores[doc_id] = TEXT_WEIGHT * Total_TF_IDF(
            doc_id, tokens_info_text, "text") + TITLE_WEIGHT * Total_TF_IDF(doc_id, tokens_info_title, "title")

    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    return sorted_scores


def get_freq_from_text_phrase(query_tokens: List[str]) -> Dict:
    tokens_info = {}

    conn = sqlite3.connect("inverted_index.db")
    cursor = conn.cursor()

    # TODO: (f)这里应该解决词项不存在的情况
    # 已解决：跳过不存在的词项
    for token in query_tokens:
        if token in token_to_id:
            token_id = token_to_id[token]
            cursor.execute(
                "SELECT doc_id, positions FROM inverted_index WHERE token=?", (token_id,))
            rows = cursor.fetchall()
            tokens_info[token] = {int(doc_id): pickle.loads(positions)
                                  for doc_id, positions in rows}
        else:
            continue
    conn.close()
    return tokens_info


def get_freq_from_title_phrase(query_tokens: List[str]) -> Dict:
    tokens_info = {}

    conn = sqlite3.connect("inverted_index.db")
    cursor = conn.cursor()
    # TODO: (f)这里应该解决词项不存在的情况
    # 已解决：跳过不存在的词项
    for token in query_tokens:
        if token in token_to_id:
            token_id = token_to_id[token]
            cursor.execute(
                "SELECT doc_id, positions FROM title_index WHERE token=?", (token_id,))
            rows = cursor.fetchall()
            tokens_info[token] = {int(doc_id): pickle.loads(positions)
                                  for doc_id, positions in rows}
        else:
            continue
    conn.close()
    return tokens_info

# 短语查询，将整个短语视作词项进行查询


def Total_TF_IDF_phrase(doc_id: int, query_tokens_info: Dict, query_tokens: List[str], doc_set: set, type: str = "text") -> float:
    """
    计算TF-IDF值
    :param doc_id: 文档ID
    :param tokens_info: 词项信息
    :param query_tokens: 查询词项
    :param doc_set: 包含所有query词项的文档集合
    :param type: 计算TF-IDF的类型，text表示计算文本的TF-IDF值，title表示计算标题的TF-IDF值
    :return: TF-IDF值
    """
    length = len(query_tokens)
    phrase_len = min(length, MAX_PHRASE_LEN)
    phrase_num = 0
    for i in range(length - phrase_len+1):
        if phrase_len == 0:
            break
        # token_id = token_to_id[query_tokens[i]]
        token = query_tokens[i]
        if token in query_tokens_info:
            if doc_id in query_tokens_info[token]:
                for place in query_tokens_info[token][doc_id]:
                    get_phrase = True
                    for j in range(1, phrase_len):
                        # token_id2 = token_to_id[query_tokens[i+j]]
                        token2 = query_tokens[i+j]
                        if token2 not in query_tokens_info or doc_id not in query_tokens_info[token2] or place+j not in query_tokens_info[token2][doc_id]:
                            get_phrase = False
                            break
                    if get_phrase:
                        phrase_num += 1
    if type == "text":
        doc_length = doc_info[doc_id]['length']
    elif type == "title":
        doc_length = len(parse_query(doc_info[doc_id]['title']))
    tf = phrase_num / doc_length
    df = 99
    idf = math.log(TOTAL_NUM / (df + 1))
    score = tf * idf
    return score

# FIXME: (f)存在bug，会什么都找不到（score一直为0）


def phrase_query(query_string: str):
    query_tokens = parse_query(query_string)
    tokens_info_text = get_freq_from_text_phrase(query_tokens)
    tokens_info_title = get_freq_from_title_phrase(query_tokens)
    doc_set_text = set()
    doc_set_title = set()
    for token in tokens_info_text:
        doc_set_text.update(tokens_info_text[token].keys())
    for token in tokens_info_title:
        doc_set_title.update(tokens_info_title[token].keys())
    doc_set = doc_set_text | doc_set_title
    scores = {}
    for doc_id in doc_set:
        scores[doc_id] = 0.0
        if doc_id in doc_set_text:
            scores[doc_id] += TEXT_WEIGHT * Total_TF_IDF_phrase(
                doc_id, tokens_info_text, query_tokens, doc_set_text, "text")
        if doc_id in doc_set_title:
            scores[doc_id] += TITLE_WEIGHT * Total_TF_IDF_phrase(
                doc_id, tokens_info_title, query_tokens, doc_set_title, "title")

    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    return sorted_scores


def wildcard_query(pattern: str):
    if pattern.startswith("*") or pattern.startswith("?"):
        tokens = reversed_trie.search_wildcard(
            pattern[::-1], reverse=True)[:MAX_TOKENS_LEN]
    else:
        tokens = trie.search_wildcard(pattern)[:MAX_TOKENS_LEN]
    scores = {}
    for token in tokens:
        if token in token_to_id:
            score = text_query(token)
            for doc_id, score_i in score:
                if doc_id not in scores:
                    scores[doc_id] = score_i
                else:
                    scores[doc_id] += score_i
    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    return sorted_scores


if __name__ == "__main__":
    while True:
        query_string = input("请输入查询语句：")
        if query_string == "exit":
            break
        if "\"" in query_string:
            sorted_scores = phrase_query(query_string)
        elif "*" in query_string or "?" in query_string:
            sorted_scores = wildcard_query(query_string)
        else:
            sorted_scores = text_query(query_string)
        top_k = 6
        for i in range(top_k):
            if i >= len(sorted_scores):
                break
            print("Score:", sorted_scores[i][1])
            print("Document ID:", sorted_scores[i][0])
            print(doc_info[sorted_scores[i][0]])
