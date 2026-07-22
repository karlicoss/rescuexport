

Tool to export your personal Rescuetime data

<!-- Keep this separator: Quarto otherwise moves the generated preamble below the following heading. -->

# Setting up

1.  install with PIP
    - `pip3 install --user git+https://github.com/karlicoss/rescuexport`

    - for export functionality: append `[export]`

    - for optional extras for logging and faster json processing: append
      `[optional]`

    - or any combination of the above, e.g. `[export,optional]`

    - alternatively, use `git clone --recursive`, or
      `git pull && git submodules update --init`. After that, you can
      use `pip3 install --editable`.

- TODO section about uv? idk

  - adhoc
    `uv tool run --from='git+https://github.com/karlicoss/rescuexport' --with 'rescuexport[export]' python3 -m rescuexport.export`

  - install doesn’t work…

        $ uv tool install --from='git+https://github.com/karlicoss/rescuexport' rescuexport 
        Resolved 1 package in 0.65ms
        Installed 1 package in 0.99ms
         + rescuexport==0.0.1.dev30 (from git+https://github.com/karlicoss/rescuexport@1963a44056856878b9235b3350051f64dbf656b5)
        No executables are provided by `rescuexport`
        [i]karlicos@kammerer:~ 01:13:14 1 $ 

- TODO how to automatically describe extras is it possible to extract
  from pyproject.toml?

- TODO just reduce number of extras??

# Exporting

Usage:

**Recommended**: create `secrets.py` keeping your API parameters, e.g.:

    key = "KEY"

After that, use:

    python3 -m rescuexport.export --secrets /path/to/secrets.py

That way you type less and have control over where you keep your
plaintext secrets.

**Alternatively**, you can pass parameters directly, e.g.

    python3 -m rescuexport.export --key <key>

However, this is verbose and prone to leaking your keys/tokens/passwords
in shell history.

I **highly** recommend checking exported files at least once just to
make sure they contain everything you expect from your export. If they
don’t, please feel free to ask or raise an issue!

# Contributing

If you want to contribute/develop this project, check out [github
actions](.github/workflows/main.yml) to see how the project is
run/tested.

Generally you should be able to run various checks via `tox`, e.g.

`uv tool run --with tox-uv tox`

## Updating README

This README is generated from a ‘literate’ Quarto
[README.qmd](README.qmd) via the following command:

`tox -e quarto`

If you want to correct something, feel free to simply update `README.md`
though, I can reconcile the changes next time I regenerate it.
