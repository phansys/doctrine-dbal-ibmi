#!/usr/bin/env bash

set -ex

echo "Installing IBM i Access ODBC driver"
(
    curl "https://public.dhe.ibm.com/software/ibmi/products/odbc/debs/dists/1.1.0/ibmi-acs-1.1.0.list" | sudo tee /etc/apt/sources.list.d/ibmi-acs-1.1.0.list
    sudo apt-get update
    sudo apt-get install -qq --yes --no-install-recommends ibm-iaccess
)
