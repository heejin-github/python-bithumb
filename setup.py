import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="python-bithumb",  # PyPI에 배포할 패키지 이름
    version="0.1.0",        # 패키지 버전
    author="youtube-jocoding",
    author_email="business@jocoding.net",
    description="A Python wrapper for Bithumb API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/youtube-jocoding/python-bithumb",
    packages=setuptools.find_packages(),
    install_requires=[
        "requests>=2.0.0",
        "pyjwt>=2.0.0",
        "pandas>=1.0.0"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    license="Apache-2.0",
    python_requires='>=3.7',
)
