"""
大文件处理客户端
集成get_rosetta_json_big_backdoor.py的功能，支持OSS下载大文件
"""

import os
import json
import shutil
import oss2
import time
import random
import requests
from typing import Dict, Any, Optional, List
from memory_client import Auth


class BigFileRosettaClient(Auth):
    """大文件Rosetta客户端，支持OSS下载"""
    
    def __init__(self, project_id, pool_id: list, save_path='./', _type=0, 
                 is_check_pool=False, username=None, password=None):
        """
        Args:
            project_id: 项目ID
            pool_id: 池子ID列表
            save_path: 保存路径
            _type: 下载类型
            is_check_pool: 是否检查池子状态
            username: 用户名
            password: 密码
        """
        super().__init__()
        
        # 使用big backdoor的API endpoint
        self.get_url = 'https://server.rosettalab.top/rosetta-service/backDoor/queryProjectExportLog'
        
        self.project_id = project_id
        self.pool_id = pool_id
        self.username = username
        self.password = password
        self.save_path = save_path
        self._type = _type
        self.is_check_pool = is_check_pool
        
        # 设置OSS配置
        self._setup_oss_config()
        
        # 初始化请求数据
        self.req_data = {"projectId": project_id, "poolId": str(pool_id[0]), "type": _type}
        if is_check_pool:
            self.req_data["poolType"] = 3
            
        # 确保保存路径存在
        os.makedirs(save_path, exist_ok=True)
        self.save_file = os.path.join(save_path, f"{self.project_id}.zip")
    
    def _setup_oss_config(self):
        """设置OSS配置"""
        # 从配置文件读取OSS认证信息
        config_path = '/Users/Apple/Documents/work/data/oss_config.json'
        
        # 如果配置文件不存在，尝试使用环境变量或默认值
        if not os.path.exists(config_path):
            # 尝试从环境变量获取
            access_key = os.getenv('OSS_ACCESS_KEY', 'your_access_key')
            secret_key = os.getenv('OSS_SECRET_KEY', 'your_secret_key')
            
            self.oss_config = {
                'access_key': access_key,
                'secret_key': secret_key
            }
            print(f"⚠️  OSS配置文件不存在，使用环境变量或默认值")
        else:
            with open(config_path, 'r') as f:
                self.oss_config = json.load(f)
        
        # 使用配置文件中的AKSK，但强制使用北京区域的endpoint
        self.auth = oss2.Auth(self.oss_config['access_key'], self.oss_config['secret_key'])
        self.bucket_name = 'rosetta-data'
        self.end_point = 'https://oss-cn-beijing.aliyuncs.com'  # 强制使用北京区域
        self.bucket = oss2.Bucket(self.auth, self.end_point, self.bucket_name)
    
    def _get_headers(self):
        """获取请求头，使用big backdoor的格式"""
        def generate_string():
            n = [''] * 20
            o = list(str(int(time.time() * 1000))[::-1])
            for i in range(20):
                e = random.randint(0, 35)
                t = str(e if e < 26 else chr(e + 87))
                n[i] = t if e % 3 else t.upper()
            for i in range(8):
                n.insert(3 * i + 2, o[i])
            return ''.join(n[::-1])

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
            import zipfile
            with zipfile.ZipFile(zip_file_path) as zip_file:
                return len(zip_file.namelist()) == 0
        except zipfile.BadZipFile:
            return True
    
    def download_file_from_oss(self, save_file_addr, oss_file_name):
        """从OSS下载文件"""
        try:
            # 处理OSS路径格式
            if oss_file_name.startswith('oss://rosetta-data/'):
                oss_file_name = oss_file_name.lstrip('oss://rosetta-data/')
            elif oss_file_name.startswith('oss://stardust-data/'):
                oss_file_name = oss_file_name.lstrip('oss://stardust-data/')
            
            print(f"正在从OSS下载: {oss_file_name}")
            print(f"保存到: {save_file_addr}")
            
            # 获取文件信息
            object_stream = self.bucket.get_object(oss_file_name)
            
            # 写入文件
            with open(save_file_addr, 'wb') as file:
                shutil.copyfileobj(object_stream, file)
            
            print(f"✅ OSS下载完成: {save_file_addr}")
            
        except Exception as e:
            print(f"❌ OSS下载失败: {str(e)}")
            raise Exception(f"OSS下载失败: {str(e)}")
    
    def get_oss_download_info(self) -> Dict[str, Any]:
        """获取OSS下载信息"""
        try:
            print(f"正在获取项目 {self.project_id} 的OSS下载信息...")
            
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
            
            data = resq.json()
            
            # 检查是否有数据
            if not data.get('data') or len(data['data']) == 0:
                raise ValueError("未找到导出日志数据，请检查项目ID和池子ID是否正确")
            
            # 获取OSS文件信息
            oss_info = data['data'][0]
            
            if 'zipFileName' not in oss_info:
                raise ValueError("OSS文件信息中未找到zipFileName字段")
            
            return oss_info
            
        except Exception as e:
            print(f"❌ 获取OSS信息失败: {str(e)}")
            raise Exception(f"获取OSS下载信息失败: {str(e)}")
    
    def download_large_file(self) -> str:
        """下载大文件的主方法"""
        try:
            # 获取OSS下载信息
            oss_info = self.get_oss_download_info()
            
            # 获取OSS文件路径
            key = oss_info['zipFileName']
            
            # 下载文件
            self.download_file_from_oss(self.save_file, key)
            
            # 检查文件
            if self._is_zip_file_empty(self.save_file) or os.path.getsize(self.save_file) == 160:
                raise ValueError("下载的文件为空或格式错误")
            
            print(f"✅ 大文件下载完成: {self.save_file}")
            print(f"文件大小: {os.path.getsize(self.save_file)} bytes")
            
            return self.save_file
            
        except Exception as e:
            print(f"❌ 大文件下载失败: {str(e)}")
            raise Exception(f"大文件下载失败: {str(e)}")
    


    def get_unziped_data(self) -> str:
        """获取解压后的数据路径
        
        Returns:
            str: 解压后的数据路径
        """
        print(f"📥 开始下载大文件项目 {self.project_id}...")
        
        # 下载大文件
        self.download_large_file()
        
        # 解压文件
        project_path = os.path.join(self.save_path, str(self.project_id))
        if os.path.exists(project_path):
            shutil.rmtree(project_path)
        
        os.makedirs(project_path, exist_ok=True)
        
        # 使用系统命令解压
        import subprocess
        result = subprocess.run(['unzip', '-q', self.save_file, '-d', project_path], 
                              capture_output=True, text=True)
        
        if result.returncode != 0:
            raise Exception(f"解压失败: {result.stderr}")
        
        print(f"✅ 文件解压完成: {project_path}")
        return project_path