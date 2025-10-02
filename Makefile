SHELL = bash
OK_MSG = \x1b[32m ✔\x1b[0m
FAIL_MSG = \x1b[31m ✖\x1b[0m
YELLOW = \x1b[33m
BLUE = \x1b[36m
RED = \x1b[31m
RESET_COLOR = \x1b[0m
ifndef PIPELINEWISE_HOME
PIPELINEWISE_HOME = $(shell pwd)
endif
VENV_DIR = ${PIPELINEWISE_HOME}/.virtualenvs

start_time:=$(shell date +%s)

PIP_ARGS="[test]"

pw_connector=

define ALL_CONNECTORS
tap-github\
tap-jira\
tap-kafka\
tap-mixpanel\
tap-mongodb\
tap-mysql\
tap-postgres\
tap-s3-csv\
tap-salesforce\
tap-slack\
tap-snowflake\
tap-twilio\
tap-zendesk\
target-s3-csv\
target-snowflake\
target-postgres\
transform-field
endef

define print_execute_time
	$(eval end_time:=`date +%s`)
	@echo
	@echo "--------------------------------------------------------------------------"
	@echo "$(1) installed successfully in $$(( $(end_time) - $(start_time) )) seconds"
	@echo "--------------------------------------------------------------------------"
endef

define clean_connectors
	echo -n "Cleaning previous installations in $(VENV_DIR)/$(1)..."
	rm -rf $(VENV_DIR)/$(1)
	@echo -e "$(OK_MSG)"
endef

define install_connectors
	echo
	echo "--------------------------------------------------------------------------"
	echo "Installing $1 connector..."
	echo "--------------------------------------------------------------------------"
	if [[ ! -d singer-connectors/$1 ]]; then\
		echo "ERROR: Directory not exists and does not look like a valid singer connector: singer-connectors: singer-connectors/$1";\
		exit 1;\
    fi
    $(call make_connector,$1,singer-connectors/$1/)
endef

define make_connector
	echo -e -n "$(RED)";
	echo "  | WARNING. The license of some connectors are different than the default PipelineWise license.";
	echo "  | Abort this installation if you do not wish to accept";
	echo;
	@echo -e -n "$(YELLOW)"
	@echo -n "Making Virtual Environment for $(1) in $(VENV_DIR)..."
	@python3 -m venv $(VENV_DIR)/$(1)
	@source $(VENV_DIR)/$(1)/bin/activate
	@echo -e "$(OK_MSG)"
	@echo -e -n "$(YELLOW)"
	@$(VENV_DIR)/$(1)/bin/python3 -m pip install --upgrade pip setuptools wheel
	@echo -e "$(RESET_COLOR)"
	@echo -n "Python setup tools updated..."
	@echo -e "$(OK_MSG)"
	@echo -e -n "$(YELLOW)"
	@test ! -s $(2)pre_requirements.txt ||\
 		($(VENV_DIR)/$(1)/bin/python3 -m pip install --use-pep517 --upgrade -r $(2)pre_requirements.txt\
 		&& echo -e "$(RESET_COLOR)"\
 		&& echo -n "Pre requirements installed..."\
 		&& echo -e "$(OK_MSG)")
	@echo -e -n "$(YELLOW)"
	@test ! -s $(2)requirements.txt ||\
		($(VENV_DIR)/$(1)/bin/python3 -m pip install --use-pep517 --upgrade -r $(2)requirements.txt\
 		&& echo -e "$(RESET_COLOR)"\
 		&& echo -n "Requirements installed..."\
 		&& echo -e "$(OK_MSG)")
	@echo -e -n "$(RESET_COLOR)"
	@test ! -s $(2)setup.py ||\
		(echo "Installing the package..."\
		&& echo -e "$(YELLOW)"\
		&& $(VENV_DIR)/$(1)/bin/python3 -m pip install --use-pep517 --upgrade -e $(2)\
		&& echo -e "$(RESET_COLOR)"\
		&& echo -n "Package installation completed..."\
		&& echo -e "$(OK_MSG)")
	@echo -e "$(RESET_COLOR)"
endef

define make_pipelinewise
	@echo -e -n "$(YELLOW)"
	@echo -n "Making Virtual Environment for $(1) in $(VENV_DIR)..."
	@python3 -m venv $(VENV_DIR)/$(1)
	@source $(VENV_DIR)/$(1)/bin/activate
	@echo -e "$(OK_MSG)"
	@echo -e -n "$(YELLOW)"
	@$(VENV_DIR)/$(1)/bin/python3 -m pip install --upgrade pip setuptools wheel
	@echo -e "$(RESET_COLOR)"
	@echo -n "Python setup tools updated..."
	@echo -e "$(OK_MSG)"
	@echo -e -n "$(YELLOW)"
	@echo "Installing the package..."
	@echo -e "$(YELLOW)"
	@$(VENV_DIR)/$(1)/bin/python3 -m pip install --use-pep517 --upgrade -e $(2)$(PIP_ARGS)
	@echo -e "$(RESET_COLOR)"
	@echo -n "Package installation completed..."
	@echo -e "$(OK_MSG)"
	@echo -e "$(RESET_COLOR)"
endef

help: .check_gettext .pw_logo
	@echo
	@echo "  Targets"
	@echo "  ======="
	@echo "     pipelinewise                                               Install the main PipelineWise component"
	@echo "     pipelinewise_no_test_extras                                Install the main Pipelinewise component without test extras"
	@echo
	@echo "     all_connectors                                             Install all connectors"
	@echo "     connectors -e pw_connector=connector1,connector2,...       Install specific connector(s)"
	@echo
	@echo "     list_all_connectors                                        Show a list of all connectors"
	@echo
	@echo "   Options"
	@echo "   ======="
	@echo "      -e pw_connector=connector1,connector2,...                 Define a list of connectors for installing or cleaning"
	@echo
	@echo "   To start CLI"
	@echo "   ============"
	@echo "      $$ source $(VENV_DIR)/pipelinewise/bin/activate"
	@echo "      $$ export PIPELINEWISE_HOME=$(PIPELINEWISE_HOME)"
	@echo "      $$ pipelinewise status"
	@echo
	@echo "--------------------------------------------------------------------------"


pipelinewise: .check_gettext .pw_logo
	$(call make_pipelinewise,pipelinewise,.)
	$(call print_execute_time,PipelineWise)

pipelinewise_no_test_extras: .set_pip_args pipelinewise

connectors:
ifeq ($(pw_connector),)
	@echo "use -e pw_connector=connector1,connector2,...."
	@exit 1
endif
	$(eval comma := ,)
	$(eval connectors_list := $(strip $(subst $(comma), ,$(pw_connector))))

	@$(foreach var,$(connectors_list), $(call install_connectors,$(var));)
	$(call print_execute_time,Connectors)

all_connectors:
	@echo "Installing all connectors..."
	@$(foreach var,$(ALL_CONNECTORS), $(call install_connectors,$(var));)
	$(call print_execute_time,All connectors)

list_all_connectors:
	@echo
	@echo "   ========================"
	@echo "   Available All Connectors"
	@echo "   ========================"
	@$(foreach var,$(ALL_CONNECTORS), $(call print_list_of_connectors,$(var));)
	@echo "   ----------------------------"

.pw_logo:
	@echo -e "$(BLUE)"
	@(CURRENT_YEAR=$(shell date +"%Y") envsubst < motd)
	@echo -e "$(RESET_COLOR)"

.check_gettext:
	@echo -n "Checking gettext..."
	@if ! ENVSUBST_LOC="$$(type -p "envsubst")" || [[ -z ENVSUBST_LOC ]]; then\
		echo -e "$(FAIL_MSG)" &&\
		echo "envsubst not found but it is required to run this script. Try to install gettext or gettext-base package" && exit 1;\
	fi
	@echo -e "$(OK_MSG)"

.set_pip_args:
	$(eval PIP_ARGS:="")