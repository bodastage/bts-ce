#!/bin/bash
#
# create mediation directories
MEDIATION_DIR=mediation/data/cm
mkdir -p $MEDIATION_DIR
mkdir -p $MEDIATION_DIR/{ericsson,huawei,nokia,zte}/{in,out,parsed,raw}

# Ericsson
mkdir -p $MEDIATION_DIR/ericsson/{raw,parsed}/{bulkcm,eaw,cnaiv2,backup}

#Huawei
mkdir -p $MEDIATION_DIR/huawei/{raw,parsed}/{gexport,nbi,mml,cfgsyn,rnp,motree,backup}

#ZTE
mkdir -p $MEDIATION_DIR/zte/{raw,parsed}/{bulkcm,excel,backup}

#Nokia
mkdir -p $MEDIATION_DIR/nokia/{raw,parsed}/{raml2,backup}

# Create mediation data
mkdir -p mediation/data/reports