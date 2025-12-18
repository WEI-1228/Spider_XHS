import json
import os
import urllib.parse
import time
import random
from datetime import datetime, timedelta
from loguru import logger
from apis.xhs_pc_apis import XHS_Apis
from xhs_utils.common_util import init
from xhs_utils.data_util import (
    handle_note_info,
    handle_user_info,
    handle_comment_info,
    download_note,
    save_to_xlsx,
    save_user_detail,
)


class Data_Spider:
    def __init__(self):
        self.xhs_apis = XHS_Apis()

    def spider_note(self, note_url: str, cookies_str: str, proxies=None):
        note_info = None
        try:
            success, msg, note_info = self.xhs_apis.get_note_info(note_url, cookies_str, proxies)
            if success:
                note_info = note_info["data"]["items"][0]
                note_info["url"] = note_url
                note_info = handle_note_info(note_info)
        except Exception as e:
            success = False
            msg = e
        logger.info(f"爬取笔记信息 {note_url}: {success}, msg: {msg}")
        return success, msg, note_info

    def spider_some_note(self, notes: list, cookies_str: str, base_path: dict, save_choice: str, excel_name: str = '', proxies=None):
        """
        爬取一些笔记的信息
        :param notes:
        :param cookies_str:
        :param base_path:
        :return:
        """
        if (save_choice == 'all' or save_choice == 'excel') and excel_name == '':
            raise ValueError('excel_name 不能为空')
        note_list = []
        for note_url in notes:
            success, msg, note_info = self.spider_note(note_url, cookies_str, proxies)
            if note_info is not None and success:
                note_list.append(note_info)
        for note_info in note_list:
            if save_choice == 'all' or 'media' in save_choice:
                download_note(note_info, base_path['media'], save_choice)
        if save_choice == 'all' or save_choice == 'excel':
            file_path = os.path.abspath(os.path.join(base_path['excel'], f'{excel_name}.xlsx'))
            save_to_xlsx(note_list, file_path)


    def spider_user_all_note(self, user_url: str, cookies_str: str, base_path: dict, save_choice: str, excel_name: str = '', proxies=None):
        """
        爬取一个用户的所有笔记
        :param user_url:
        :param cookies_str:
        :param base_path:
        :return:
        """
        note_list = []
        try:
            success, msg, all_note_info = self.xhs_apis.get_user_all_notes(user_url, cookies_str, proxies)
            if success:
                logger.info(f'用户 {user_url} 作品数量: {len(all_note_info)}')
                for simple_note_info in all_note_info:
                    note_url = f"https://www.xiaohongshu.com/explore/{simple_note_info['note_id']}?xsec_token={simple_note_info['xsec_token']}"
                    note_list.append(note_url)
            if save_choice == 'all' or save_choice == 'excel':
                excel_name = user_url.split('/')[-1].split('?')[0]
            self.spider_some_note(note_list, cookies_str, base_path, save_choice, excel_name, proxies)
        except Exception as e:
            success = False
            msg = e
        logger.info(f'爬取用户所有视频 {user_url}: {success}, msg: {msg}')
        return note_list, success, msg

    def spider_some_search_note(self, query: str, require_num: int, cookies_str: str, base_path: dict, save_choice: str, sort_type_choice=0, note_type=0, note_time=0, note_range=0, pos_distance=0, geo: dict = None,  excel_name: str = '', proxies=None):
        """
            指定数量搜索笔记，设置排序方式和笔记类型和笔记数量
            :param query 搜索的关键词
            :param require_num 搜索的数量
            :param cookies_str 你的cookies
            :param base_path 保存路径
            :param sort_type_choice 排序方式 0 综合排序, 1 最新, 2 最多点赞, 3 最多评论, 4 最多收藏
            :param note_type 笔记类型 0 不限, 1 视频笔记, 2 普通笔记
            :param note_time 笔记时间 0 不限, 1 一天内, 2 一周内天, 3 半年内
            :param note_range 笔记范围 0 不限, 1 已看过, 2 未看过, 3 已关注
            :param pos_distance 位置距离 0 不限, 1 同城, 2 附近 指定这个必须要指定 geo
            返回搜索的结果
        """
        note_list = []
        try:
            success, msg, notes = self.xhs_apis.search_some_note(query, require_num, cookies_str, sort_type_choice, note_type, note_time, note_range, pos_distance, geo, proxies)
            if success:
                notes = list(filter(lambda x: x['model_type'] == "note", notes))
                logger.info(f'搜索关键词 {query} 笔记数量: {len(notes)}')
                for note in notes:
                    note_url = f"https://www.xiaohongshu.com/explore/{note['id']}?xsec_token={note['xsec_token']}"
                    note_list.append(note_url)
            if save_choice == 'all' or save_choice == 'excel':
                excel_name = query
            self.spider_some_note(note_list, cookies_str, base_path, save_choice, excel_name, proxies)
        except Exception as e:
            success = False
            msg = e
        logger.info(f'搜索关键词 {query} 笔记: {success}, msg: {msg}')
        return note_list, success, msg

    def spider_user_complete_data(
        self,
        user_input: str,
        cookies_str: str,
        base_path: dict,
        save_choice: str = "all",
        excel_name: str = "",
        proxies=None,
        days_limit: int = 365,  # 默认只爬取最近365天（近一年），设为 None 爬全部
    ):
        user_info = None
        note_list = []
        all_comments = []
        success = False
        msg = ""

        try:
            # 1. 解析用户输入
            if "xiaohongshu.com" in user_input:
                user_url = user_input
                user_id = urllib.parse.urlparse(user_url).path.split("/")[-1].split("?")[0]
            else:
                user_id = user_input
                user_url = f"https://www.xiaohongshu.com/user/profile/{user_id}"
                logger.warning("仅提供user_id，将尝试构造URL")

            logger.info(f"开始爬取用户: {user_id}")
            logger.info(f"时间过滤: {'最近 ' + str(days_limit) + ' 天' if days_limit else '全部笔记'}")

            # 2. 获取用户信息
            success, msg, user_info_raw = self.xhs_apis.get_user_info(user_id, cookies_str, proxies)
            if not success:
                raise Exception(f"获取用户信息失败: {msg}")
            user_info = handle_user_info(user_info_raw["data"], user_id)
            logger.info(f"✓ 用户信息获取成功: {user_info['nickname']}")

            # 保存用户信息
            if save_choice == "all" or save_choice == "excel":
                if not excel_name:
                    excel_name = f"{user_info['nickname']}_{user_id}"
                user_excel_path = os.path.abspath(os.path.join(base_path["excel"], f"{excel_name}_用户信息.xlsx"))
                save_to_xlsx([user_info], user_excel_path, type="user")
                logger.info(f"✓ 用户信息已保存: {user_excel_path}")

            # 3. 获取所有笔记列表
            success, msg, all_note_info = self.xhs_apis.get_user_all_notes(user_url, cookies_str, proxies)
            if not success:
                raise Exception(f"获取笔记列表失败: {msg}")
            logger.info(f"✓ 原始找到 {len(all_note_info)} 篇笔记")

            # 时间范围下限（仅用于“最终上传时间”判断）
            cutoff_time = None
            if days_limit is not None:
                cutoff_time = datetime.now() - timedelta(days=days_limit)
                logger.info(f"✓ 将按上传时间过滤，只保留最近 {days_limit} 天的笔记")

            # 4. 遍历笔记：每篇只请求两次（详情 + 评论），媒体立即下载
            note_id_set = set()
            for idx, simple_note_info in enumerate(all_note_info, 1):
                note_id = simple_note_info["note_id"]
                note_id_set.add(note_id)
                xsec_token = simple_note_info.get("xsec_token", "")
                note_url = f"https://www.xiaohongshu.com/explore/{note_id}?xsec_token={xsec_token}"
                logger.info(f"[{idx}/{len(all_note_info)}] 处理笔记 {note_id}")

                # === 第一次请求：获取笔记详情 ===
                try:
                    success_note, _, note_info_raw = self.xhs_apis.get_note_info(note_url, cookies_str, proxies)
                    if not success_note or not note_info_raw.get("data", {}).get("items"):
                        logger.warning(" ✗ 笔记详情获取失败")
                        continue

                    raw_item = note_info_raw["data"]["items"][0]
                    raw_item["url"] = note_url
                    note_info = handle_note_info(raw_item)

                    # 按“上传时间”做最终时间过滤，确保不会出现 2023 年的笔记混入近一年结果
                    if cutoff_time is not None:
                        try:
                            upload_dt = datetime.strptime(note_info["upload_time"], "%Y-%m-%d %H:%M:%S")
                            if upload_dt < cutoff_time:
                                logger.info(
                                    f"  - 跳过笔记 {note_id}，上传时间 {note_info['upload_time']} 不在最近 {days_limit} 天内"
                                )
                                continue
                        except Exception as e:
                            logger.warning(f"  - 上传时间解析失败，仍保留该笔记: {e}")

                    note_list.append(note_info)
                    logger.info(f" ✓ 笔记详情成功: {note_info['title'][:30]}...")

                    # 立即下载媒体（利用刚获取的最新 note_info，无需二次请求）
                    if save_choice == "all" or "media" in save_choice:
                        try:
                            download_note(note_info, base_path["media"], save_choice)
                            logger.info(f" ✓ 媒体下载完成")
                        except Exception as e:
                            logger.warning(f" ✗ 媒体下载失败: {e}")

                except Exception as e:
                    logger.error(f" ✗ 笔记详情异常: {e}")
                    continue

                # === 第二次请求：获取全部评论 ===
                try:
                    success_comment, _, comments_raw = self.xhs_apis.get_note_all_comment(note_url, cookies_str, proxies)
                    if success_comment and comments_raw:
                        comment_count = 0
                        for comment_raw in comments_raw:
                            comment_raw["note_id"] = note_id
                            comment_raw["note_url"] = note_url
                            all_comments.append(handle_comment_info(comment_raw))
                            comment_count += 1
                            if comment_raw.get("sub_comments"):
                                for sub in comment_raw["sub_comments"]:
                                    sub["note_id"] = note_id
                                    sub["note_url"] = note_url
                                    all_comments.append(handle_comment_info(sub))
                                    comment_count += 1
                        logger.info(f" ✓ 获取 {comment_count} 条评论")
                except Exception as e:
                    logger.error(f" ✗ 获取评论异常: {e}")

            # 5. 保存笔记总表
            if note_list and (save_choice == "all" or save_choice == "excel"):
                note_excel_path = os.path.abspath(os.path.join(base_path["excel"], f"{excel_name}_笔记.xlsx"))
                save_to_xlsx(note_list, note_excel_path, type="note")
                logger.info(f"✓ 笔记总表已保存: {note_excel_path}")

            # 6. 保存每个笔记的独立评论Excel（匹配媒体文件夹）
            if (save_choice == "all" or save_choice == "excel") and all_comments:
                media_root = base_path["media"]
                matched_count = 0
                remaining_ids = note_id_set.copy()

                if os.path.exists(media_root):
                    for user_folder_name in os.listdir(media_root):
                        user_folder_path = os.path.join(media_root, user_folder_name)
                        if not os.path.isdir(user_folder_path):
                            continue
                        for note_folder_name in os.listdir(user_folder_path):
                            note_folder_path = os.path.join(user_folder_path, note_folder_name)
                            if not os.path.isdir(note_folder_path):
                                continue

                            matched_note_id = None
                            for nid in remaining_ids:
                                if nid in note_folder_name:
                                    matched_note_id = nid
                                    break

                            if matched_note_id:
                                note_comments = [c for c in all_comments if c["note_id"] == matched_note_id]
                                if note_comments:
                                    comment_path = os.path.join(note_folder_path, "评论.xlsx")
                                    try:
                                        save_to_xlsx(note_comments, comment_path, type="comment")
                                        logger.info(f"✓ 笔记 {matched_note_id} 评论已保存 ({len(note_comments)}条)")
                                        matched_count += 1
                                    except Exception as e:
                                        logger.error(f"✗ 评论保存失败 {matched_note_id}: {e}")
                                remaining_ids.remove(matched_note_id)

                logger.info(f"✓ 本次成功为 {matched_count} 篇笔记保存独立评论文件")
                if remaining_ids:
                    logger.warning(f"未匹配到媒体文件夹的笔记: {remaining_ids}")

            # 7. 保存评论汇总表
            if all_comments and (save_choice == "all" or save_choice == "excel"):
                total_comment_path = os.path.abspath(os.path.join(base_path["excel"], f"{excel_name}_所有笔记评论汇总.xlsx"))
                save_to_xlsx(all_comments, total_comment_path, type="comment")
                logger.info(f"✓ 评论汇总表已保存: {total_comment_path}")

            success = True
            msg = f"成功: 笔记{len(note_list)}篇, 评论{len(all_comments)}条"

        except Exception as e:
            success = False
            msg = f"失败: {str(e)}"
            logger.error(msg)

        return user_info, note_list, all_comments, success, msg


def load_user_ids_from_file(file_path="user_ids.txt"):
    user_ids = []
    if not os.path.exists(file_path):
        logger.error(f"用户ID文件不存在: {file_path}")
        logger.info("请创建 user_ids.txt 文件，每行一个用户ID或完整URL")
        return user_ids
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                user_ids.append(line)
        logger.info(f"读取到 {len(user_ids)} 个用户")
    except Exception as e:
        logger.error(f"读取文件失败: {e}")
    return user_ids


if __name__ == "__main__":
    cookies_str, base_path = init()
    data_spider = Data_Spider()

    save_choice = "all"

    user_ids = load_user_ids_from_file("user_ids.txt")
    if not user_ids:
        logger.error("未找到有效用户ID，程序退出")
        exit(1)

    total = len(user_ids)
    success_count = fail_count = 0

    logger.info("=" * 60)
    logger.info(f"开始批量爬取 {total} 个用户（默认仅近1年笔记）")
    logger.info("=" * 60)

    for idx, user_input in enumerate(user_ids, 1):
        logger.info(f"\n{'='*60}")
        logger.info(f"[{idx}/{total}] 处理用户: {user_input}")
        logger.info(f"{'='*60}")

        try:
            _, _, _, success, msg = data_spider.spider_user_complete_data(
                user_input,
                cookies_str,
                base_path,
                save_choice=save_choice,
                days_limit=365,        # 修改这里控制时间范围，None 为全部
            )
            if success:
                success_count += 1
                logger.info("✓ 本用户爬取成功")
            else:
                fail_count += 1
                logger.error(f"✗ 本用户爬取失败: {msg}")
        except Exception as e:
            fail_count += 1
            logger.error(f"✗ 处理异常: {e}")

    logger.info("\n" + "=" * 60)
    logger.info("批量爬取完成！")
    logger.info(f"总计: {total}  成功: {success_count}  失败: {fail_count}")
    logger.info("=" * 60)