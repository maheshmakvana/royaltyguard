from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="royaltyguard",
    version="1.2.0",
    author="",
    description="Creator royalty tracking and streaming fraud detection — bot streams, zero-rate payouts, DSP reconciliation, earnings forecasting, fraud pattern library",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/royaltyguard-py/royaltyguard",
    packages=find_packages(exclude=["tests*"]),
    python_requires=">=3.8",
    install_requires=[
        "pydantic>=2.0",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Intended Audience :: Developers",
    ],
    keywords=[
        "royalty tracking", "streaming fraud detection", "music royalties python",
        "bot streams detection", "spotify royalty audit",
        "creator royalties", "indie artist tools",
        "royalty fraud", "streaming manipulation detection",
        "music rights management python", "DSP royalty reconciliation python",
        "royalty underpayment detection", "streaming earnings forecast python",
        "royalty statement audit", "music distributor fraud",
        "creator economy analytics python",
    ],
)
