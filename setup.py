from setuptools import setup


def readme():
    with open('README.md') as f:
        return f.read().strip()


setup(
    name='genesapi_pipeline',
    version='0.9',
    description='The data pipeline to turn tables from GENESIS into a graphql api',
    long_description=readme(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Topic :: Scientific/Engineering',
        'Topic :: Utilities'
    ],
    url='https://github.com/datenguide/genesapi-pipeline',
    author='Simon Wörpel',
    author_email='simon.woerpel@medienrevolte.de',
    license='MIT',
    packages=['genesapi'],
    entry_points={
        'console_scripts': [
            'genesapi=genesapi.entry:main'
        ]
    },
    install_requires=[
        'pandas',
        'pyyaml',
        'python-frontmatter',
        'awesome-slugify',
        'zeep',
        'regenesis'
    ],
    # FIXME see README.md about regenesis install note
    # dependency_links=[
    #     'https://github.com/datenguide/regenesis/tarball/master#egg=regenesis-0.1'
    # ],
    zip_safe=False
)
