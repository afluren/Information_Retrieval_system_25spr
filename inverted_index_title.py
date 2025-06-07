# 文档id：
# 所在文件的id*100000+所在文件中的行号
# 解码：文档id/100000，得到所在文件id；文档id%100000，得到所在文件中的行号。

import sqlite3
import jieba
import re
import json
from typing import List, Dict
from collections import defaultdict
from multiprocessing import Process, Manager
import pickle
from tqdm import tqdm
import os
import gc


def tokenize(text: str) -> list[str]:
    tokens = []
    for word in jieba.lcut(text):
        # 提取英文/数字
        eng_subs = re.findall(r"[a-zA-Z0-9]+", word)
        if eng_subs:
            tokens.extend([w.lower() for w in eng_subs])
        else:
            # 判断是否为中文字符
            if re.fullmatch(r"[\u4e00-\u9fff]+", word):
                tokens.append(word)
    return tokens


def process_files(files_id: int, token_to_id: Dict, id_to_token: List[str], doc_info: Dict, token_lock, stopwords: set):
    for i in range(1):
        file_id = files_id
        with open(f"deduped_html/nankai{file_id}.jsonl", "r", encoding="utf-8") as f:
            total_lines = sum(1 for _ in f)
        with open(f"deduped_html/nankai{file_id}.jsonl", "r", encoding="utf-8") as f:
            # 行号，从1开始
            index = 0
            token_invertd_index = {}
            pbar = tqdm(
                total=total_lines, desc=f"Processing nankai{file_id}.jsonl", position=file_id)
            while True:
                index += 1
                # 获取当前网页数据偏移量
                offset = f.tell()
                line = f.readline()
                if not line:
                    break
                web_data = json.loads(line)
                # 按照规则生成文档id
                doc_id = file_id * 100000 + index

                # 处理网页数据
                full_text = web_data["title"]
                if full_text is None:
                    continue
                # 分词
                tokens = tokenize(full_text)
                tokens = [token for token in tokens if token not in stopwords]
                # 保存倒排索引，一个词一个词的加入倒排索引
                test_set = set(range(len(tokens)))
                for j in test_set:
                    token = tokens[j]
                    # 词不存在，加入词典
                    if token not in token_to_id:
                        with token_lock:
                            if token not in token_to_id:
                                token_to_id[token] = len(id_to_token)
                                id_to_token.append(token)
                    # 词不存在，加入倒排索引
                    token_id = token_to_id[token]
                    if token_id not in token_invertd_index:
                        token_invertd_index[token_id] = {}
                    if doc_id not in token_invertd_index[token_id]:
                        token_invertd_index[token_id][doc_id] = {
                            "freq": 1,
                            "positions": [j]
                        }
                    else:
                        token_invertd_index[token_id][doc_id]["freq"] += 1
                        token_invertd_index[token_id][doc_id]["positions"].append(
                            j)
                # if index % 50 == 0:
                # print(
                #     f"file_id: {file_id}, index: {index}, Processed {doc_id}, title: {web_data['title']}")
                pbar.update(1)  # 每读一行，手动更新进度条

            pbar.close()
            # 保存倒排索引到磁盘
            with open(f"inverted_index_title/nankai{file_id}.pkl", "wb") as f:
                pickle.dump(token_invertd_index, f)


def build_inverted_index():
    # 多进程处理文件
    num_workers = 32
    with Manager() as manager:
        # 停用词
        with open("baidu_stopwords.txt", "r", encoding="utf-8") as f:
            stopwords = set(line.strip() for line in f if line.strip())
        with open("inverted_index/token_to_id.pkl", "rb") as f:
            token_to_id = pickle.load(f)
        with open("inverted_index/id_to_token.pkl", "rb") as f:
            id_to_token = pickle.load(f)
        with open("inverted_index/doc_info.pkl", "rb") as f:
            doc_info = pickle.load(f)
        # 词典，token -> token_id
        token_to_id = manager.dict(token_to_id)
        # 列表，token_id -> token
        id_to_token = manager.list(id_to_token)
        # 锁
        token_lock = manager.Lock()

        process = []
        for i in range(num_workers):
            p = Process(target=process_files, args=(
                i, token_to_id, id_to_token, doc_info, token_lock, stopwords))
            p.start()
            process.append(p)

        for p in process:
            p.join()
        # 保存词典、列表、文档信息
        with open("inverted_index_title/token_to_id.pkl", "wb") as f:
            pickle.dump(dict(token_to_id), f)
        with open("inverted_index_title/id_to_token.pkl", "wb") as f:
            pickle.dump(list(id_to_token), f)


def merge():
    final_inverted_index = {}
    for i in tqdm(range(32)):
        with open(f"inverted_index/nankai{i}.pkl", "rb") as f:
            inverted_index = pickle.load(f)
        length = len(inverted_index)
        pbar = tqdm(
            total=length, desc=f"Processing nankai{i}.jsonl", position=i)
        for token_id, info in inverted_index.items():
            if token_id not in final_inverted_index:
                final_inverted_index[token_id] = info
            else:
                final_inverted_index[token_id].update(info)
            pbar.update(1)
        pbar.close()
        del inverted_index
        gc.collect()

    # 保存最终的倒排索引
    with open("inverted_index/inverted_index.pkl", "wb") as f:
        pickle.dump(final_inverted_index, f)


def merge_indexes_to_sqlite(index_dir, output_db):
    conn = sqlite3.connect(output_db)
    cur = conn.cursor()

    # 创建表
    cur.execute('''
        CREATE TABLE title_index (
        token TEXT,
        doc_id TEXT,
        freq INTEGER,
        positions BLOB,
        PRIMARY KEY (token, doc_id)
    )
    ''')
    conn.commit()

    for i in range(32):
        with open(f"{index_dir}/nankai{i}.pkl", "rb") as f:
            inv = pickle.load(f)

        length = len(inv)
        pbar = tqdm(total=length, desc=f"Processing nankai{i}.pkl", position=i)

        for token, doc_dict in inv.items():
            for doc_id, doc_info in doc_dict.items():
                # 查询是否已经有这个 (token, doc_id)
                cur.execute(
                    "SELECT freq, positions FROM title_index WHERE token=? AND doc_id=?", (token, doc_id))
                row = cur.fetchone()

                if row:
                    # 解码原有信息
                    old_freq, old_positions = row[0], pickle.loads(row[1])
                    new_freq = old_freq + doc_info["freq"]
                    new_positions = old_positions + doc_info["positions"]
                else:
                    new_freq = doc_info["freq"]
                    new_positions = doc_info["positions"]

                cur.execute("REPLACE INTO title_index VALUES (?, ?, ?, ?)",
                            (token, doc_id, new_freq, pickle.dumps(new_positions)))

            pbar.update(1)
        pbar.close()
        conn.commit()
        del inv
        gc.collect()

    conn.close()


if __name__ == '__main__':
    print("Building inverted index...")
    merge_indexes_to_sqlite("inverted_index_title", "inverted_index.db")
    print("Done.")
    # build_inverted_index()
