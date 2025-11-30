clean: # Remove workspace files
	@find . -name "__pycache__" -exec rm -rf {} +
	@rm -rf ./.pytest_cache
	@rm -rf ./htmlcov
	@rm -rf tlaloc.egg-info
	@rm -rf dist/
	@rm -rf build/
	@rm -rf __blobstorage__
	@rm -rf .ipynb_checkpoints
	@rm -rf .mypy_cache
	@rm -rf spark-warehouse/
	@rm -rf .ruff_cache
	@rm -rf report.xml
	@rm -rf .coverage
	@echo "✨ done"