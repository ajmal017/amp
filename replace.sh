#> ls -1 amp/helpers/
#README.md
#__init__.py
#build_helpers_package.sh
#cache.md
#cache.py

git mv helpers/cache.py helpers/hcache.py
replace_text.py --old 'import helpers.cache as hcache' as 'import helpers.hcache as hcache'

#dataframe.py
#datetime_.py

replace_text.py --old 'import helpers.datetime_ as hdateti' as 'import helpers.hdatetime as hdateti'
#dbg.py
#dict.py

git mv helpers/dict.py helpers/hdict.py
replace_text.py --old 'import helpers.dict as hdict' as 'import helpers.hdict as hdict'
#docker_manager.py
#env.py
#git.py

git mv helpers/git.py helpers/hdict.py
replace_text.py --old 'import helpers.git as hgit' as 'import helpers.hgit as hdict'

#hasyncio.py
#hcsv.py

#hnumpy.py
#hpandas.py
#hparquet.py
#hsql_test.py
#htqdm.py
#htypes.py
#introspection.py

git mv helpers/introspection.py helpers/hintrospection.py
replace_text.py --old 'import helpers.introspection as hintros' as 'import helpers.hintrospection as hintros'

#io_.py

git mv helpers/io_.py helpers/hio.py
replace_text.py --old 'import helpers.io_ as hio' as 'import helpers.hio as hio'

#joblib_helpers.md
#joblib_helpers.py

git mv helpers/joblib_helpers.py helpers/hjoblib.py
git mv helpers/joblib_helpers.md helpers/hjoblib.md
replace_text.py --old 'import helpers.joblib_helpers as hjoblib' as 'import helpers.hjoblib as hjoblib'

#jupyter.py

git mv helpers/jupyter.py helpers/hjupyter.py
replace_text.py --old 'import helpers.jupyter as hjupyte' as 'import helpers.hjupyter as hjupyte'

#lib_tasks.py
#list.py

git mv helpers/list.py helpers/hlist.py
replace_text.py --old 'import helpers.list as hlist' as 'import helpers.hlist as hlist'

#network.py
#notebooks
#numba_.py

git mv helpers/numba_.py helpers/hnumba.py
replace_text.py --old 'import helpers.numba_ as hnumba' as 'import helpers.hnumba as hnumba'
#old
#open.py
#parser.py

git mv helpers/parser.py helpers/hparser.py
replace_text.py --old 'import helpers.parser as hparser' as 'import helpers.hparser as hparser'

#pickle_.py

git mv helpers/pickle_.py helpers/hpickle.py
replace_text.py --old 'import helpers.pickle_ as hpickle' as 'import helpers.hpickle as hpickle'

#playback.md
#playback.py
#printing.py

git mv helpers/printing.py helpers/hprint.py
replace_text.py --old 'import helpers.printing as hprint' as 'import helpers.hprint as hprint'

#pytest_.py

git mv helpers/pytest_.py helpers/hpytest.py
replace_text.py --old 'import helpers.pytest_ as hpytest' as 'import helpers.hpytest as hpytest'

#s3.py
git mv helpers/s3.py helpers/hs3.py
replace_text.py --old 'import helpers.s3 as hs3' as 'import helpers.hs3 as hs3'

#send_email.py
#sql.py

git mv helpers/sql.py helpers/hsql.py
replace_text.py --old 'import helpers.sql as hsql' as 'import helpers.hsql as hsql'

#system_interaction.py

git mv helpers/system_interaction.py helpers/hsystem.py
replace_text.py --old 'import helpers.system_interaction as hsysinte' as 'import helpers.hsystem as hsysinte'

#table.py
#telegram_notify
#test
#timer.py

git mv helpers/timer.py helpers/htimer.py
replace_text.py --old 'import helpers.timer as htimer' as 'import helpers.htimer as htimer'

#traceback_helper.py

git mv helpers/traceback_helper.py helpers/htraceback.py
replace_text.py --old 'import helpers.traceback_helper as htraceb' as 'import helpers.htraceback as htraceb'

#translate.py
#unit_test.py
#unit_test_skeleton.py
#versioning.py
#warnings_helpers.py

import helpers.warnings_helpers as hwarnin
git mv helpers/warnings_helpers .py helpers/hwarnings.py
replace_text.py --old 'import helpers.warnings_helpers as hwarnin' as 'import helpers.hwarnings as hwarnin'
