from setuptools import setup


def getVersion():
    import re, os
    r_version = re.compile(r'version\s=\s"(.*?)"')
    version_py = os.path.abspath(os.path.join(__file__,
                                 '../crudset/version.py'))
    return r_version.search(open(version_py, 'r').read()).groups()[0]


setup(
    url='none',
    author='Matt Haggard',
    author_email='haggardii@gmail.com',
    name='crudset',
    version=getVersion(),
    packages=[
        'crudset', 'crudset.test'
    ],
    install_requires=[
        'alchimia',
        'SQLAlchemy==0.8.3',
    ]
)
