from setuptools import setup, find_packages # type: ignore


def main():
    pkg = 'rescuexport'
    pkgs = find_packages('src')
    return setup(
        name=pkg,
        zip_safe=False,
        packages=pkgs,
        package_dir={'': 'src'},
        package_data={pkg: ['py.typed']},

        url='',
        author='',
        author_email='',
        description='',

        install_requires=[
            'requests',
            'backoff',
        ],
        extras_require={
            'testing': ['pytest'],
            'linting': ['pytest', 'mypy'],
        },
    )


if __name__ == '__main__':
    main()
