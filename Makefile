SHELL = bash
OK_MSG = \x1b[32m ✔\x1b[0m
FAIL_MSG = \x1b[31m ✖\x1b[0m
YELLOW = \x1b[33m
BLUE = \x1b[36m
RED = \x1b[31m
RESET_COLOR = \x1b[0m
PIPELINEWISE_HOME = $(shell pwd)
VENV_DIR = ${PIPELINEWISE_HOME}/.virtualenvs

python ?= "python3"

start_time:=$(shell date +%s)

PIP_ARGS="[test]"

pw_connector=

define DEFAULT_CONNECTORS
tap-jira\
tap-kafka\
tap-mysql\
tap-postgres\
tap-s3-csv\
tap-salesforce\
tap-snowflake\
tap-zendesk\
tap-mongodb\
tap-github\
tap-slack\
tap-mixpanel\
tap-twilio\
target-s3-csv\
target-snowflake\
target-redshift\
target-postgres\
target-bigquery\
transform-field
endef

define EXTRA_CONNECTORS
tap-oracle\
tap-zuora\
tap-google-analytics\
tap-shopify
endef

define print_installed_connectors
	@echo
	@echo "--------------------------------------------------------------------------"
	@echo "Installed components:"
	@echo "--------------------------------------------------------------------------"
	@echo
	@echo "Component            Version"
	@echo "-------------------- -------"
	@for i in $(shell ls $(VENV_DIR)); do\
		VERSION=`$(VENV_DIR)/$$i/bin/python3 -m pip list | grep "$$i[[:space:]]" | awk '{print $$2}'`;\
		printf "%-20s %s\n" $$i "$$VERSION";\
	done;
	@echo "-------------------- -------"
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

define check_license
	@echo "Checking license..."
	@echo -e "$(YELLOW)"
	@$(VENV_DIR)/$(1)/bin/python3 -m pip install pip-licenses==3.5.3
	@echo -e "$(RESET_COLOR)"
	$(eval PKG_NAME:=`$(VENV_DIR)/$(1)/bin/pip-licenses|grep "$(1)[[:space:]]"| awk '{print $$$$1}'`)
	$(eval PKG_VERSION:=`$(VENV_DIR)/$(1)/bin/pip-licenses | grep "$(1)[[:space:]]" | awk '{print $$$$2}'`)
	$(eval PKG_LICENSE:=`$(VENV_DIR)/$(1)/bin/pip-licenses --from mixed | grep "$(1)[[:space:]]" | awk '{for (i=1; i<=NF-2; i++) $$$$i = $$$$(i+2); NF-=2; print}'`)

	$(eval MAIN_LICENSE:="Apache Software License")

	@if [[ "$(PKG_LICENSE)" != $(MAIN_LICENSE) && "$(PKG_LICENSE)" != "UNKNOWN" ]]; then\
		echo -e "$(RED)";\
		echo;\
        echo "  | $(PKG_NAME) ($(PKG_VERSION)) is licensed under $(PKG_LICENSE)";\
        echo "  |";\
        echo "  | WARNING. The license of this connector is different than the default PipelineWise license ($(MAIN_LICENSE)).";\
        if [[ "$(ACCEPT_LICENSES)" != "YES" ]]; then\
            echo "  | You need to accept the connector's license agreement to proceed.";\
            echo "  |";\
            read -r -p "  | Do you accept the [$(PKG_LICENSE)] license agreement of $(PKG_NAME) connector? [y/N] " response;\
			if [[ $$response != "y" && $$response != "Y" ]]; then\
				echo;\
				echo -e "$(RESET_COLOR)";\
				echo "EXIT. License agreement not accepted!";\
				exit 1;\
			fi;\
		else\
			echo "  | You automatically accepted this license agreement by running this script with acceptlicenses=YES option.";\
        fi;\
        echo;\
	fi
	@echo -e "$(RESET_COLOR)"
	@echo -n "License accepted..."
	@echo -e "$(OK_MSG)"
endef

define make_virtualenv
	@echo -n "Making Virtual Environment for $(1) in $(VENV_DIR)..."
	@echo -e -n "$(YELLOW)"
	@test -d $(VENV_DIR)/$(1) || $(python) -m venv $(VENV_DIR)/$(1)
	@source $(VENV_DIR)/$(1)/bin/activate
	@echo -e "$(OK_MSG)"
	@echo -e -n "$(YELLOW)"
	@$(VENV_DIR)/$(1)/bin/python3 -m pip install --upgrade pip setuptools wheel
	@echo -e "$(RESET_COLOR)"
	@echo -n "Python setup tools updated..."
	@echo -e "$(OK_MSG)"
	@echo -e -n "$(YELLOW)"
	@test ! -s $(2)pre_requirements.txt ||\
 		($(VENV_DIR)/$(1)/bin/pip install --upgrade -r $(2)pre_requirements.txt\
 		&& echo -e "$(RESET_COLOR)"\
 		&& echo -n "Pre requirements installed..."\
 		&& echo -e "$(OK_MSG)")
	@echo -e -n "$(YELLOW)"
	@test ! -s $(2)requirements.txt ||\
		($(VENV_DIR)/$(1)/bin/pip install --upgrade -r $(2)requirements.txt\
 		&& echo -e "$(RESET_COLOR)"\
 		&& echo -n "Requirements installed..."\
 		&& echo -e "$(OK_MSG)")
	@echo -e -n "$(RESET_COLOR)"
	@test ! -s $(2)setup.py ||\
		(echo "Installing the package..."\
		 && echo -e "$(YELLOW)"\
		 && $(VENV_DIR)/$(1)/bin/python3 -m pip install --upgrade -e .$(PIP_ARGS)\
		 && echo -e "$(RESET_COLOR)"\
		 && echo -n "Package installation completed..."\
		 && echo -e "$(OK_MSG)")
	@echo -e "$(RESET_COLOR)"
	$(call check_license,$(1))
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
    $(call make_virtualenv,$1,singer-connectors/$1/)
endef

define print_list_of_connectors
	echo "   $1"
endef

help: .check_gettext .pw_logo
	@echo
	@echo "  Targets"
	@echo "  ======="
	@echo "     pipelinewise                                               Install the main PipelineWise component"
	@echo "     pipelinewise_no_test_extras                                Install the main Pipelinewise component without test extras"
	@echo
	@echo "     all_connectors                                             Install all connectors"
	@echo "     default_connectors                                         Install default connectors"
	@echo "     extra_connectors                                           Install only extra connectors"
	@echo "     connectors -e pw_connector=connector1,connector2,...       Install specific connector(s)"
	@echo
	@echo "     list_installed_components                                  Show a list of installed components"
	@echo "     list_default_connectors                                    Show a list of available default connectors"
	@echo "     list_extra_connectors                                      Show a list of available extra connectors"
	@echo
	@echo "     clean_all                                                  Clean all installed components"
	@echo "     clean -e pw_connector=connector1,connector2,...            Clean a specific connector(s)"
	@echo
	@echo "   Options"
	@echo "   ======="
	@echo "      -e pw_connector=connector1,connector2,...                 Define a list of connectors for installing or cleaning"
	@echo "      -e pw_acceptlicenses=y/Y/Yes/YES                          Forcing to accept the licenses automatically"
	@echo
	@echo "   To start CLI"
	@echo "   ============"
	@echo "      $$ source $(VENV_DIR)/pipelinewise/bin/activate"
	@echo "      $$ export PIPELINEWISE_HOME=$(PIPELINEWISE_HOME)"
	@echo "      $$ pipelinewise status"
	@echo
	@echo "--------------------------------------------------------------------------"


pipelinewise: .check_gettext .pw_logo
	$(call make_virtualenv,pipelinewise)
	$(call print_execute_time,PipelineWise)

pipelinewise_no_test_extras: .set_pip_args pipelinewise

clean_all:
	@echo -n "Cleaning previous installations in $(VENV_DIR)..."
	@rm -rf $(VENV_DIR)
	@echo -e "$(OK_MSG)"

clean:
ifeq ($(pw_connector),)
	@echo "use -e pw_connector=connector1,connector2,...."
	@exit 1
endif
	$(eval space:= )
	$(eval space+= )
	$(eval comma:=,)
	$(eval connectors_list:=$(subst $(comma),$(space),$(pw_connector)))

	@$(foreach var,$(connectors_list), $(call clean_connectors,$(var));)

connectors: .check_license_env_var
ifeq ($(pw_connector),)
	@echo "use -e pw_connector=connector1,connector2,...."
	@exit 1
endif
	$(eval space:= )
	$(eval space+= )
	$(eval comma:=,)
	$(eval connectors_list:=$(subst $(comma),$(space),$(pw_connector)))

	@$(foreach var,$(connectors_list), $(call install_connectors,$(var));)
	$(call print_execute_time,Connectors)


all_connectors: default_connectors extra_connectors
	@echo "Install all connectors..."
	$(call print_execute_time,All connectors)

default_connectors: .check_license_env_var
	@echo "Installing default connectors..."
	@$(foreach var,$(DEFAULT_CONNECTORS), $(call install_connectors,$(var));)
	$(call print_execute_time,Default connectors)

extra_connectors: .check_license_env_var
	@echo "Installing extra connectors..."
	@$(foreach var,$(EXTRA_CONNECTORS), $(call install_connectors,$(var));)
	$(call print_execute_time,Extra connectors)


list_installed_components:
	$(call print_installed_connectors)

list_default_connectors:
	@echo
	@echo "   ============================"
	@echo "   Available Default Connectors"
	@echo "   ============================"
	@$(foreach var,$(DEFAULT_CONNECTORS), $(call print_list_of_connectors,$(var));)
	@echo "   ----------------------------"

list_extra_connectors:
	@echo
	@echo "   ============================"
	@echo "   Available Extra Connectors"
	@echo "   ============================"
	@$(foreach var,$(EXTRA_CONNECTORS), $(call print_list_of_connectors,$(var));)
	@echo "   ----------------------------"

.pw_logo:
	@echo -e "$(BLUE)"
	@(CURRENT_YEAR=$(shell date +"%Y") envsubst < motd)
	@echo -e "$(RESET_COLOR)"

.check_license_env_var:
	$(eval ACCEPT_LICENSES:=NO)
ifeq ($(pw_acceptlicenses),y)
	$(eval ACCEPT_LICENSES:=YES)
endif
ifeq ($(pw_acceptlicenses),Y)
	$(eval ACCEPT_LICENSES:=YES)
endif
ifeq ($(pw_acceptlicenses),Yes)
	$(eval ACCEPT_LICENSES:=YES)
endif
ifeq ($(pw_acceptlicenses),YES)
	$(eval ACCEPT_LICENSES:=YES)
endif

.check_gettext:
	@echo -n "Checking gettext..."
	@if ! ENVSUBST_LOC="$$(type -p "envsubst")" || [[ -z ENVSUBST_LOC ]]; then\
		echo -e "$(FAIL_MSG)" &&\
		echo "envsubst not found but it is required to run this script. Try to install gettext or gettext-base package" && exit 1;\
	fi
	@echo -e "$(OK_MSG)"

.set_pip_args:
	$(eval PIP_ARGS:="")
