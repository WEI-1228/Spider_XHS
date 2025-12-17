import json
import os
import urllib.parse
from loguru import logger
from apis.xhs_pc_apis import XHS_Apis
from xhs_utils.common_util import init
from xhs_utils.data_util import handle_note_info, handle_user_info, handle_comment_info, download_note, save_to_xlsx, save_user_detail


class Data_Spider():
    def __init__(self):
        self.xhs_apis = XHS_Apis()

    def spider_note(self, note_url: str, cookies_str: str, proxies=None):
        """
        爬取一个笔记的信息
        :param note_url:
        :param cookies_str:
        :return:
        """
        note_info = None
        try:
            success, msg, note_info = self.xhs_apis.get_note_info(note_url, cookies_str, proxies)
            if success:
                note_info = note_info['data']['items'][0]
                note_info['url'] = note_url
                note_info = handle_note_info(note_info)
        except Exception as e:
            success = False
            msg = e
        logger.info(f'爬取笔记信息 {note_url}: {success}, msg: {msg}')
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

    def spider_user_complete_data(self, user_input: str, cookies_str: str, base_path: dict, save_choice: str = 'all', excel_name: str = '', proxies=None):
        """
        爬取用户的完整数据：用户信息 + 所有笔记 + 所有笔记的评论
        :param user_input: 用户ID或用户URL（如果只有user_id，会尝试构造URL，但建议使用完整的user_url）
        :param cookies_str: cookies字符串
        :param base_path: 保存路径字典 {'excel': excel路径, 'media': 媒体路径}
        :param save_choice: 保存选项 'all': 保存所有, 'excel': 只保存Excel, 'media': 只保存媒体文件
        :param excel_name: Excel文件名（如果不提供，会使用用户ID）
        :param proxies: 代理设置
        :return: (user_info, note_list, all_comments, success, msg)
        """
        user_info = None
        note_list = []
        all_comments = []
        success = False
        msg = ''

        try:
            # 1. 解析用户输入，获取user_id和user_url
            user_id = None
            user_url = None
            
            if 'xiaohongshu.com' in user_input:
                # 输入的是完整URL
                user_url = user_input
                url_parse = urllib.parse.urlparse(user_url)
                user_id = url_parse.path.split("/")[-1].split('?')[0]
            else:
                # 输入的是user_id，尝试构造基本URL
                user_id = user_input
                user_url = f'https://www.xiaohongshu.com/user/profile/{user_id}'
                logger.warning(f'只提供了user_id，将尝试使用基本URL。如果失败，请提供完整的user_url（包含xsec_token）')

            logger.info(f'开始爬取用户数据: user_id={user_id}')

            # 2. 获取用户信息
            logger.info('正在获取用户信息...')
            success, msg, user_info_raw = self.xhs_apis.get_user_info(user_id, cookies_str, proxies)
            if not success:
                raise Exception(f'获取用户信息失败: {msg}')
            
            user_info = handle_user_info(user_info_raw['data'], user_id)
            logger.info(f'✓ 用户信息获取成功: {user_info["nickname"]}')

            # 保存用户信息到Excel
            if save_choice == 'all' or save_choice == 'excel':
                if excel_name == '':
                    excel_name = f"{user_info['nickname']}_{user_id}"
                user_excel_path = os.path.abspath(os.path.join(base_path['excel'], f'{excel_name}_用户信息.xlsx'))
                save_to_xlsx([user_info], user_excel_path, type='user')
                logger.info(f'✓ 用户信息已保存到: {user_excel_path}')

            # 3. 获取用户所有笔记
            logger.info('正在获取用户所有笔记...')
            success, msg, all_note_info = self.xhs_apis.get_user_all_notes(user_url, cookies_str, proxies)
            if not success:
                raise Exception(f'获取用户笔记失败: {msg}')
            
            logger.info(f'✓ 找到 {len(all_note_info)} 篇笔记')

            # 4. 遍历每个笔记，获取详细信息和评论
            for idx, simple_note_info in enumerate(all_note_info, 1):
                note_id = simple_note_info['note_id']
                xsec_token = simple_note_info.get('xsec_token', '')
                note_url = f"https://www.xiaohongshu.com/explore/{note_id}?xsec_token={xsec_token}"

                logger.info(f'[{idx}/{len(all_note_info)}] 正在处理笔记: {note_id}')

                # 4.1 获取笔记详细信息
                try:
                    success_note, msg_note, note_info_raw = self.xhs_apis.get_note_info(note_url, cookies_str, proxies)
                    if success_note and note_info_raw and 'data' in note_info_raw and 'items' in note_info_raw['data']:
                        note_info_raw = note_info_raw['data']['items'][0]
                        note_info_raw['url'] = note_url
                        note_info = handle_note_info(note_info_raw)
                        note_list.append(note_info)
                        logger.info(f'  ✓ 笔记信息获取成功: {note_info["title"][:30]}...')
                    else:
                        logger.warning(f'  ✗ 笔记信息获取失败: {msg_note}')
                        continue
                except Exception as e:
                    logger.error(f'  ✗ 获取笔记信息异常: {str(e)}')
                    continue

                # 4.2 获取笔记的所有评论（包括一级和二级评论）
                try:
                    success_comment, msg_comment, comments_raw = self.xhs_apis.get_note_all_comment(note_url, cookies_str, proxies)
                    if success_comment and comments_raw:
                        comment_count = 0
                        # 处理每个一级评论
                        for comment_raw in comments_raw:
                            comment_raw['note_id'] = note_id
                            comment_raw['note_url'] = note_url
                            try:
                                comment_info = handle_comment_info(comment_raw)
                                all_comments.append(comment_info)
                                comment_count += 1
                            except Exception as e:
                                logger.warning(f'  ✗ 处理一级评论异常: {str(e)}')
                                continue
                            
                            # 处理该一级评论下的所有二级评论（子评论）
                            if 'sub_comments' in comment_raw and comment_raw['sub_comments']:
                                for sub_comment_raw in comment_raw['sub_comments']:
                                    sub_comment_raw['note_id'] = note_id
                                    sub_comment_raw['note_url'] = note_url
                                    try:
                                        sub_comment_info = handle_comment_info(sub_comment_raw)
                                        all_comments.append(sub_comment_info)
                                        comment_count += 1
                                    except Exception as e:
                                        logger.warning(f'  ✗ 处理二级评论异常: {str(e)}')
                                        continue
                        
                        logger.info(f'  ✓ 获取到 {len(comments_raw)} 条一级评论，共 {comment_count} 条评论（含二级评论）')
                    else:
                        logger.warning(f'  ✗ 评论获取失败: {msg_comment}')
                except Exception as e:
                    logger.error(f'  ✗ 获取评论异常: {str(e)}')

            # 5. 保存笔记数据
            if note_list and (save_choice == 'all' or save_choice == 'excel'):
                note_excel_path = os.path.abspath(os.path.join(base_path['excel'], f'{excel_name}_笔记.xlsx'))
                save_to_xlsx(note_list, note_excel_path, type='note')
                logger.info(f'✓ 笔记数据已保存到: {note_excel_path}')

            # 下载笔记媒体文件
            if note_list and (save_choice == 'all' or 'media' in save_choice):
                for note_info in note_list:
                    try:
                        download_note(note_info, base_path['media'], save_choice)
                    except Exception as e:
                        logger.warning(f'下载笔记媒体文件失败 {note_info["note_id"]}: {str(e)}')

            # 6. 保存评论数据
            if all_comments and (save_choice == 'all' or save_choice == 'excel'):
                comment_excel_path = os.path.abspath(os.path.join(base_path['excel'], f'{excel_name}_评论.xlsx'))
                save_to_xlsx(all_comments, comment_excel_path, type='comment')
                logger.info(f'✓ 评论数据已保存到: {comment_excel_path}')

            success = True
            msg = f'成功爬取用户数据: 用户信息1条, 笔记{len(note_list)}篇, 评论{len(all_comments)}条'
            logger.info(f'✓ {msg}')

        except Exception as e:
            success = False
            msg = f'爬取用户数据失败: {str(e)}'
            logger.error(msg)

        return user_info, note_list, all_comments, success, msg

def load_user_ids_from_file(file_path='user_ids.txt'):
    """
    从文件中读取用户ID列表
    :param file_path: 用户ID文件路径
    :return: 用户ID列表
    """
    user_ids = []
    if not os.path.exists(file_path):
        logger.error(f'用户ID文件不存在: {file_path}')
        logger.info('请创建 user_ids.txt 文件，每行一个用户ID或用户URL')
        return user_ids
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                # 跳过空行和注释行
                if not line or line.startswith('#'):
                    continue
                user_ids.append(line)
        logger.info(f'从 {file_path} 读取到 {len(user_ids)} 个用户ID')
    except Exception as e:
        logger.error(f'读取用户ID文件失败: {str(e)}')
    
    return user_ids


if __name__ == '__main__':
    """
        此文件为爬虫的入口文件，可以直接运行
        apis/xhs_pc_apis.py 为爬虫的api文件，包含小红书的全部数据接口，可以继续封装
        apis/xhs_creator_apis.py 为小红书创作者中心的api文件
        感谢star和follow
        
        使用方法：
        1. 创建 user_ids.txt 文件，每行一个用户ID或用户URL
        2. 运行: python main.py
        3. 数据会保存到 datas/excel_datas 和 datas/media_datas 目录
    """

    cookies_str, base_path = init()
    data_spider = Data_Spider()
    
    """
        save_choice: all: 保存所有的信息, media: 保存视频和图片（media-video只下载视频, media-image只下载图片，media都下载）, excel: 保存到excel
        save_choice 为 excel 或者 all 时，excel_name 不能为空
    """
    save_choice = 'all'  # 保存选项：'all', 'excel', 'media'
    
    # 从文件读取用户ID列表
    user_ids = load_user_ids_from_file('user_ids.txt')
    
    if not user_ids:
        logger.error('没有找到有效的用户ID，程序退出')
        logger.info('请在 user_ids.txt 文件中添加用户ID，每行一个')
        exit(1)
    
    # 统计信息
    total_users = len(user_ids)
    success_count = 0
    fail_count = 0
    
    logger.info(f'=' * 60)
    logger.info(f'开始批量爬取用户数据，共 {total_users} 个用户')
    logger.info(f'=' * 60)
    
    # 遍历每个用户ID进行爬取
    for idx, user_input in enumerate(user_ids, 1):
        logger.info(f'\n{"=" * 60}')
        logger.info(f'[{idx}/{total_users}] 开始处理用户: {user_input}')
        logger.info(f'{"=" * 60}')
        
        try:
            user_info, note_list, all_comments, success, msg = data_spider.spider_user_complete_data(
                user_input, cookies_str, base_path, save_choice=save_choice, excel_name='', proxies=None
            )
            
            if success:
                success_count += 1
                logger.info(f'✓ [{idx}/{total_users}] 用户 {user_input} 爬取成功')
                if user_info:
                    logger.info(f'  - 用户: {user_info.get("nickname", "未知")}')
                logger.info(f'  - 笔记: {len(note_list)} 篇')
                logger.info(f'  - 评论: {len(all_comments)} 条')
            else:
                fail_count += 1
                logger.error(f'✗ [{idx}/{total_users}] 用户 {user_input} 爬取失败: {msg}')
        
        except KeyboardInterrupt:
            logger.warning('\n用户中断程序')
            break
        except Exception as e:
            fail_count += 1
            logger.error(f'✗ [{idx}/{total_users}] 用户 {user_input} 处理异常: {str(e)}')
            continue
    
    # 输出最终统计
    logger.info(f'\n{"=" * 60}')
    logger.info(f'批量爬取完成！')
    logger.info(f'总计: {total_users} 个用户')
    logger.info(f'成功: {success_count} 个')
    logger.info(f'失败: {fail_count} 个')
    logger.info(f'{"=" * 60}')
