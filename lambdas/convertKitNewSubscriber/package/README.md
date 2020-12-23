# Environment
=============
conda deactivate (to get out of (base))
pipenv shell (to get into pipenv shell) (exit to leave)

# Build: (uses Makefile)
=======
" https://krzysztofzuraw.com/blog/2016/makefiles-in-python-projects.html
sed -n '/\[packages\]/,/^$/p' Pipfile (need to keep working on this)
make build

# Tests
=======
" https://stackoverflow.com/questions/6323860/sibling-package-imports
" https://binx.io/blog/2018/11/11/aws-lambda-unit-testing/
pipenv run pytest

# Mocks
=====
https://stackoverflow.com/questions/9559963/unit-testing-a-python-app-that-uses-the-requests-library
