### Install spack

1. `git clone -c feature.manyFiles=true https://github.com/spack/spack.git`
2. ` . spack/share/spack/setup-env.sh`

### Git clone the spack-packages
* `git clone git@github.com:GueroudjiAmal/mochi-spack-packages.git`
* `git clone git@github.com:GueroudjiAmal/MofkaDask.git`



### Setup Spack
1. `spack env create mofka MofkaDask/polaris_spack.yaml` 
2. `spack env activate mofka`
3. `spack repo add mochi-spack-packages`
4. *Temporary Fix until this is patched in the Darshan main branch* - Open `/spack/var/spack/repos/builtin/packages/darshan-util/package.py` and comment out all the functions `tests_log_path`, `_copy_test_inputs(self)`, and `test_parser` (line 100 down).  
5.  `spack install`



