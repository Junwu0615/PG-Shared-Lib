import os, sys
from pathlib import Path


def get_logger_name(file_path, project_root):
    try:
        # 1. 取得相對路徑
        relative_path = Path(file_path).resolve().relative_to(Path(project_root).resolve())
        # 2. 移除副檔名並轉換為點號分隔
        parts = list(relative_path.with_suffix('').parts)
        # 3. 轉大寫並用點連接
        return '.'.join(parts).upper()

    except Exception:
        # 萬一發生預期外的路徑錯誤，回傳原始檔案名作為保險
        return os.path.basename(file_path).upper()


def get_project_root(marker: str='.git'):
    """TODO 1. 從當前檔案往上找，直到發現標記物為止"""
    current_path = os.path.abspath(__file__)
    while current_path != os.path.dirname(current_path): # 沒撞到磁碟根目錄前不停止
        current_path = os.path.dirname(current_path)
        if marker in os.listdir(current_path):
            return current_path

    # TODO 2. 如果都沒找到，就回傳當前檔案的父目錄作為保險
    return os.path.dirname(os.path.abspath(__file__))


def setting_sys_path(marker_list: list):
    for marker in marker_list:
        _get_root = str(get_project_root(marker))
        if _get_root not in sys.path:
            sys.path.insert(0, _get_root) # TODO 放在最前面，優先權最高
    return sys.path

def get_sys_path(marker: str):
    _get_root = str(get_project_root(marker))
    if _get_root not in sys.path:
        sys.path.insert(0, _get_root) # TODO 放在最前面，優先權最高
    return _get_root


# TODO 自動定位並加入路徑
SYS_PATH_ROOT = setting_sys_path(['src', '.git'])

GET_PATH_ROOT = get_sys_path('.git')