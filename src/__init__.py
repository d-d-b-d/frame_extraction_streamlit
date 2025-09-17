"""
Rosetta数据下载与拆帧工具包

这是一个集成的工具包，用于从Rosetta平台下载数据并进行拆帧处理。
"""

__version__ = "1.0.0"
__author__ = "Data Team"

from .downloader import RosettaDownloader
from .extractor import FrameExtractor
from .pipeline import ExtractionPipeline