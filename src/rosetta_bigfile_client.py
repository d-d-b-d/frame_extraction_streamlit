"""
Rosetta大文件API客户端
从get_rosetta_json_big_backdoor.py移植，提供OSS文件下载功能
"""

import random
import time
import requests
import zipfile
import os
import json
import shutil
from typing import Optional, Dict, Any


class Auth:
    """认证类"""
    
    def __init__(self, use_dev=False):
        """
        Args:
            use_dev: 是否使用开发环境
        """
        self.login_url = 'https://server.rosettalab.top/rosetta-service/user/login'

    def get_authorize(self, username=None, password=None):
        """获取认证token
        
        Args:
            username: 用户名，如果为None需要用户输入
            password: 密码，如果为None需要用户输入
        """
        # 优先使用传入的用户名密码，其次尝试从配置文件读取，最后提示用户输入
        username = username or os.getenv('ROSETTA_USERNAME')
        password = password or os.getenv('ROSETTA_PASSWORD')
        
        if not username or not password:
            raise ValueError("请提供Rosetta认证信息：\n"
                           "1. 在config.yaml中配置rosetta.username和rosetta.password\n"
                           "2. 或者设置环境变量ROSETTA_USERNAME和ROSETTA_PASSWORD\n"
                           "3. 或者在运行时通过命令行参数提供")

        data = {
            "username": username,
            "password": password
        }
        data = json.dumps(data, separators=(',', ':'))
        
        response = requests.post(self.login_url, headers={
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
        }, data=data)
        
        response_data = response.json()
        
        if 'data' not in response_data or 'tokenValue' not in response_data['data']:
            error_msg = "认证失败"
            if 'message' in response_data:
                error_msg += f": {response_data['message']}"
            elif 'msg' in response_data:
                error_msg += f": {response_data['msg']}"
            else:
                error_msg += f": {response_data}"
            raise ValueError(error_msg)
        
        return response_data['data']['tokenValue']


class RosettaBigFileClient(Auth):
    """Rosetta大文件客户端 - 通过OSS下载大文件"""
    
    def __init__(self, project_id, pool_id: list, save_path='./', _type=0, 
                 is_check_pool=False, use_dev=False, username=None, password=None):
        """
        Args:
            project_id: 项目ID
            pool_id: 池子ID列表
            save_path: 保存路径
            _type: 下载类型，0为平台导出，1为任务导出
            is_check_pool: 是否检查池子状态
            use_dev: 是否使用开发环境
            username: 用户名
            password: 密码
        """
        super().__init__(use_dev)
        
        self.get_url = 'https://server.rosettalab.top/rosetta-service/backDoor/queryProjectExportLog'

        self.project_id = project_id
        self.pool_id = pool_id
        self.username = username
        self.password = password
        
        # 注意：大文件接口的pool_id需要转换为字符串
        self.req_data = {"projectId": project_id, "poolId": str(pool_id[0]), "type": _type}
        if is_check_pool:
            self.req_data["poolType"] = 3
            
        self.save_path = save_path
        self.save_file = os.path.join(save_path, f"{self.project_id}.zip")
        os.makedirs(save_path, exist_ok=True)

    def _generate_session_id(self):
        """生成会话ID"""
        n = [''] * 20
        o = list(str(int(time.time() * 1000))[::-1])
        for i in range(20):
            e = random.randint(0, 35)
            t = str(e if e < 26 else chr(e + 87))
            n[i] = t if e % 3 else t.upper()
        for i in range(8):
            n.insert(3 * i + 2, o[i])
        return ''.join(n[::-1])

    def _get_headers(self):
        """获取请求头"""
        return {
            'userId': '170152',
            'teamId': '0',
            'Authorize': self.get_authorize(self.username, self.password),
            'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'Cache-Control': 'no-cache',
            'Host': 'server.rosettalab.top',
            'Connection': 'keep-alive',
        }

    def _is_zip_file_empty(self, zip_file_path):
        """检查zip文件是否为空"""
        try:
            with zipfile.ZipFile(zip_file_path) as zip_file:
                return len(zip_file.namelist()) == 0
        except zipfile.BadZipFile:
            return True

    def _get_oss_config(self):
        """获取OSS配置"""
        try:
            config_path = '/Users/Apple/Documents/work/data/oss_config.json'
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            raise ValueError(f"无法读取OSS配置文件: {str(e)}")

    def _download_from_oss(self, oss_file_name: str, save_path: str) -> bool:
        """从OSS下载文件
        
        Args:
            oss_file_name: OSS文件路径
            save_path: 本地保存路径
            
        Returns:
            bool: 下载是否成功
        """
        try:
            # 延迟导入OSS库，避免不必要的依赖
            import oss2
            
            # 获取OSS配置
            oss_config = self._get_oss_config()
            
            # 创建OSS认证和桶对象
            auth = oss2.Auth(oss_config['access_key'], oss_config['secret_key'])
            bucket_name = 'rosetta-data'
            end_point = 'https://oss-cn-beijing.aliyuncs.com'
            bucket = oss2.Bucket(auth, end_point, bucket_name)
            
            # 处理OSS文件路径
            if oss_file_name.startswith('oss://rosetta-data/'):
                oss_file_name = oss_file_name.lstrip('oss://rosetta-data/')
            
            # 下载文件
            object_stream = bucket.get_object(oss_file_name)
            with open(save_path, 'wb') as file:
                shutil.copyfileobj(object_stream, file)
            
            print(f"OSS文件下载成功: {oss_file_name} -> {save_path}")
            return True
            
        except Exception as e:
            print(f"OSS文件下载失败: {str(e)}")
            return False

    def test(self):
        """测试接口"""
        if not os.path.exists(f'{self.save_path}/{self.project_id}/'):
            os.makedirs(f'{self.save_path}/{self.project_id}/')
        
        resq = requests.post(self.get_url, json=self.req_data, headers=self._get_headers())
        self.save_file = f"{self.save_path}/{self.project_id}/{self.pool_id[0]}.zip"
        return resq

    def unzip_data(self):
        """解压数据"""
        if not self.save_path:
            raise AttributeError("没有save_path")
        
        # 使用Python内置的zipfile解压
        with zipfile.ZipFile(self.save_file, 'r') as zip_ref:
            zip_ref.extractall(os.path.join(self.save_path, str(self.project_id)))

    def delete_data(self):
        """删除数据"""
        if os.path.exists(f'{self.save_path}/{self.project_id}.zip'):
            os.remove(f'{self.save_path}/{self.project_id}.zip')
        if os.path.exists(f'{self.save_path}/{self.project_id}'):
            shutil.rmtree(f'{self.save_path}/{self.project_id}')

    def delete_data_wy(self):
        """删除所有数据"""
        if os.path.exists(self.save_path):
            shutil.rmtree(self.save_path)

    def get_data(self):
        """下载数据（通过OSS）"""
        print("开始获取OSS文件信息...")
        
        # 第一步：获取OSS文件信息
        resq = requests.post(self.get_url, json=self.req_data, headers=self._get_headers())
        
        print(f"API响应状态码: {resq.status_code}")
        
        if resq.status_code != 200:
            error_msg = f"API请求失败，状态码: {resq.status_code}"
            try:
                error_json = resq.json()
                if 'message' in error_json:
                    error_msg += f", 错误信息: {error_json['message']}"
            except:
                error_msg += f", 响应: {resq.text[:200]}"
            raise ValueError(error_msg)
        
        # 解析响应数据
        data = resq.json()
        if 'data' not in data or len(data['data']) == 0:
            raise ValueError("API响应中没有文件数据")
        
        # 获取OSS文件路径
        oss_file_name = data['data'][0]['zipFileName']
        print(f"获取到OSS文件: {oss_file_name}")
        
        # 第二步：从OSS下载文件
        if not self._download_from_oss(oss_file_name, self.save_file):
            raise ValueError("OSS文件下载失败")
        
        # 检查文件是否有效
        if self._is_zip_file_empty(self.save_file) or os.path.getsize(self.save_file) == 160:
            raise ValueError("下载的数据为空或格式错误，请检查项目ID和池子ID是否正确")
        
        print(f"数据下载完成，文件大小: {os.path.getsize(self.save_file)} bytes")

    def get_unziped_data(self):
        """下载并解压数据"""
        print("开始下载数据...")
        self.get_data()
        print("数据下载完成，开始解压...")
        self.unzip_data()
        print("数据解压完成")

    def get_unziped_data_specified_path(self):
        """下载并解压到指定路径"""
        self.get_data()
        if not self.save_path:
            raise AttributeError("没有save_path")
        
        with zipfile.ZipFile(self.save_file, 'r') as zip_ref:
            zip_ref.extractall(self.save_path)

    def generate_dirs(self):
        """生成目录结构"""
        os.makedirs(f"{self.save_path}/source", exist_ok=True)
        os.makedirs(f"{self.save_path}/before", exist_ok=True)
        os.makedirs(f"{self.save_path}/after", exist_ok=True)

    def get_file_and_generate_dir(self):
        """下载数据并生成目录"""
        self.get_data()
        self.generate_dirs()
        
        # 使用Python解压
        with zipfile.ZipFile(self.save_file, 'r') as zip_ref:
            zip_ref.extractall(f'{self.save_path}/source')