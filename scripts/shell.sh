#!/bin/sh

# Assuming code for the dev setup is not kept in /src, we should be fine.
if [ -d "/src" ]; then
    # Container
    # Install admin tools.
    apk update
    apk add postgresql-client redis zsh nano less git
    pip3 install pyreadline ipython

    # Uninstall and reinstall as a development environment.
    pip3 uninstall -y archives
    python3 setup.py develop

    # Setup aliases and drop to a shell.
    echo "alias redis-cli='redis-cli -h $REDIS_HOST -p $REDIS_PORT'" >> ~/.zshrc
    zsh -i
else
    # Boot up an admin container!
    docker-compose -f ../docker-compose.yml run --rm -u 0 web scripts/shell.sh
fi