from pandas import DataFrame

run_907_all_ratio = [

    ['907', 'ratio', 'mcr2_13', 'IP', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '001'", 'M', 'MDCD_PD_AMT', "STC_CD= '001'"],

    ['907', 'ratio', 'mcr2_17', 'IP', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '058'", 'M', 'MDCD_PD_AMT', "STC_CD= '058'"],
    ['907', 'ratio', 'mcr2_18', 'IP', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '060'", 'M', 'MDCD_PD_AMT', "STC_CD= '060'"],
    ['907', 'ratio', 'mcr2_19', 'IP', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '084'", 'M', 'MDCD_PD_AMT', "STC_CD= '084'"],
    ['907', 'ratio', 'mcr2_20', 'IP', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '086'", 'M', 'MDCD_PD_AMT', "STC_CD= '086'"],
    ['907', 'ratio', 'mcr2_21', 'IP', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '090'", 'M', 'MDCD_PD_AMT', "STC_CD= '090'"],
    ['907', 'ratio', 'mcr2_22', 'IP', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '091'", 'M', 'MDCD_PD_AMT', "STC_CD= '091'"],
    ['907', 'ratio', 'mcr2_23', 'IP', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '092'", 'M', 'MDCD_PD_AMT', "STC_CD= '092'"],
    ['907', 'ratio', 'mcr2_24', 'IP', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '093'", 'M', 'MDCD_PD_AMT', "STC_CD= '093'"],

    ['907', 'ratio', 'mcr6_9', 'LT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '044'", 'M', 'MDCD_PD_AMT', "STC_CD= '044'"],
    ['907', 'ratio', 'mcr6_10', 'LT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '045'", 'M', 'MDCD_PD_AMT', "STC_CD= '045'"],
    ['907', 'ratio', 'mcr6_11', 'LT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '046'", 'M', 'MDCD_PD_AMT', "STC_CD= '046'"],
    ['907', 'ratio', 'mcr6_12', 'LT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '047'", 'M', 'MDCD_PD_AMT', "STC_CD= '047'"],
    ['907', 'ratio', 'mcr6_13', 'LT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '048'", 'M', 'MDCD_PD_AMT', "STC_CD= '048'"],
    ['907', 'ratio', 'mcr6_14', 'LT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '050'", 'M', 'MDCD_PD_AMT', "STC_CD= '050'"],
    ['907', 'ratio', 'mcr6_15', 'LT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '059'", 'M', 'MDCD_PD_AMT', "STC_CD= '059'"],
    ['907', 'ratio', 'mcr6_16', 'LT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '009'", 'M', 'MDCD_PD_AMT', "STC_CD= '009'"],

    ['907', 'ratio', 'mcr12_79', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '010'", 'M', 'MDCD_PD_AMT', "STC_CD= '010'"],
    ['907', 'ratio', 'mcr12_80', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '011'", 'M', 'MDCD_PD_AMT', "STC_CD= '011'"],
    ['907', 'ratio', 'mcr12_81', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '115'", 'M', 'MDCD_PD_AMT', "STC_CD= '115'"],
    ['907', 'ratio', 'mcr12_82', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '012'", 'M', 'MDCD_PD_AMT', "STC_CD= '012'"],
    ['907', 'ratio', 'mcr12_83', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '127'", 'M', 'MDCD_PD_AMT', "STC_CD= '127'"],
    ['907', 'ratio', 'mcr12_84', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '013'", 'M', 'MDCD_PD_AMT', "STC_CD= '013'"],
    ['907', 'ratio', 'mcr12_85', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '014'", 'M', 'MDCD_PD_AMT', "STC_CD= '014'"],
    ['907', 'ratio', 'mcr12_86', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '015'", 'M', 'MDCD_PD_AMT', "STC_CD= '015'"],
    ['907', 'ratio', 'mcr12_87', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '016'", 'M', 'MDCD_PD_AMT', "STC_CD= '016'"],
    ['907', 'ratio', 'mcr12_88', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '017'", 'M', 'MDCD_PD_AMT', "STC_CD= '017'"],
    ['907', 'ratio', 'mcr12_89', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '018'", 'M', 'MDCD_PD_AMT', "STC_CD= '018'"],

    ['907', 'ratio', 'mcr12_90', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '019'", 'M', 'MDCD_PD_AMT', "STC_CD= '019'"],
    ['907', 'ratio', 'mcr12_91', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '002'", 'M', 'MDCD_PD_AMT', "STC_CD= '002'"],
    ['907', 'ratio', 'mcr12_92', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '020'", 'M', 'MDCD_PD_AMT', "STC_CD= '020'"],
    ['907', 'ratio', 'mcr12_93', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '021'", 'M', 'MDCD_PD_AMT', "STC_CD= '021'"],
    ['907', 'ratio', 'mcr12_94', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '022'", 'M', 'MDCD_PD_AMT', "STC_CD= '022'"],
    ['907', 'ratio', 'mcr12_95', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '023'", 'M', 'MDCD_PD_AMT', "STC_CD= '023'"],
    ['907', 'ratio', 'mcr12_96', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '024'", 'M', 'MDCD_PD_AMT', "STC_CD= '024'"],
    ['907', 'ratio', 'mcr12_97', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '025'", 'M', 'MDCD_PD_AMT', "STC_CD= '025'"],
    ['907', 'ratio', 'mcr12_98', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '026'", 'M', 'MDCD_PD_AMT', "STC_CD= '026'"],
    ['907', 'ratio', 'mcr12_99', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '027'", 'M', 'MDCD_PD_AMT', "STC_CD= '027'"],

    ['907', 'ratio', 'mcr12_100', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '028'", 'M', 'MDCD_PD_AMT', "STC_CD= '028'"],
    ['907', 'ratio', 'mcr12_101', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '029'", 'M', 'MDCD_PD_AMT', "STC_CD= '029'"],
    ['907', 'ratio', 'mcr12_102', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '003'", 'M', 'MDCD_PD_AMT', "STC_CD= '003'"],
    ['907', 'ratio', 'mcr12_103', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '030'", 'M', 'MDCD_PD_AMT', "STC_CD= '030'"],
    ['907', 'ratio', 'mcr12_104', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '031'", 'M', 'MDCD_PD_AMT', "STC_CD= '031'"],
    ['907', 'ratio', 'mcr12_105', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '032'", 'M', 'MDCD_PD_AMT', "STC_CD= '032'"],
    ['907', 'ratio', 'mcr12_106', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '035'", 'M', 'MDCD_PD_AMT', "STC_CD= '035'"],
    ['907', 'ratio', 'mcr12_107', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '036'", 'M', 'MDCD_PD_AMT', "STC_CD= '036'"],
    ['907', 'ratio', 'mcr12_108', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '037'", 'M', 'MDCD_PD_AMT', "STC_CD= '037'"],
    ['907', 'ratio', 'mcr12_109', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '038'", 'M', 'MDCD_PD_AMT', "STC_CD= '038'"],

    ['907', 'ratio', 'mcr12_110', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '039'", 'M', 'MDCD_PD_AMT', "STC_CD= '039'"],
    ['907', 'ratio', 'mcr12_111', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '004'", 'M', 'MDCD_PD_AMT', "STC_CD= '004'"],
    ['907', 'ratio', 'mcr12_112', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '040'", 'M', 'MDCD_PD_AMT', "STC_CD= '040'"],
    ['907', 'ratio', 'mcr12_113', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '041'", 'M', 'MDCD_PD_AMT', "STC_CD= '041'"],
    ['907', 'ratio', 'mcr12_114', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '042'", 'M', 'MDCD_PD_AMT', "STC_CD= '042'"],
    ['907', 'ratio', 'mcr12_115', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '043'", 'M', 'MDCD_PD_AMT', "STC_CD= '043'"],
    ['907', 'ratio', 'mcr12_116', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '049'", 'M', 'MDCD_PD_AMT', "STC_CD= '049'"],
    ['907', 'ratio', 'mcr12_117', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '005'", 'M', 'MDCD_PD_AMT', "STC_CD= '005'"],
    ['907', 'ratio', 'mcr12_118', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '050'", 'M', 'MDCD_PD_AMT', "STC_CD= '050'"],
    ['907', 'ratio', 'mcr12_119', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '051'", 'M', 'MDCD_PD_AMT', "STC_CD= '051'"],

    ['907', 'ratio', 'mcr12_120', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '052'", 'M', 'MDCD_PD_AMT', "STC_CD= '052'"],
    ['907', 'ratio', 'mcr12_121', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '053'", 'M', 'MDCD_PD_AMT', "STC_CD= '053'"],
    ['907', 'ratio', 'mcr12_122', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '054'", 'M', 'MDCD_PD_AMT', "STC_CD= '054'"],
    ['907', 'ratio', 'mcr12_123', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '055'", 'M', 'MDCD_PD_AMT', "STC_CD= '055'"],
    ['907', 'ratio', 'mcr12_124', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '056'", 'M', 'MDCD_PD_AMT', "STC_CD= '056'"],
    ['907', 'ratio', 'mcr12_125', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '057'", 'M', 'MDCD_PD_AMT', "STC_CD= '057'"],
    ['907', 'ratio', 'mcr12_126', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '006'", 'M', 'MDCD_PD_AMT', "STC_CD= '006'"],
    ['907', 'ratio', 'mcr12_127', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '061'", 'M', 'MDCD_PD_AMT', "STC_CD= '061'"],
    ['907', 'ratio', 'mcr12_128', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '062'", 'M', 'MDCD_PD_AMT', "STC_CD= '062'"],
    ['907', 'ratio', 'mcr12_129', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '063'", 'M', 'MDCD_PD_AMT', "STC_CD= '063'"],
    ['907', 'ratio', 'mcr12_130', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '064'", 'M', 'MDCD_PD_AMT', "STC_CD= '064'"],
    ['907', 'ratio', 'mcr12_131', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '065'", 'M', 'MDCD_PD_AMT', "STC_CD= '065'"],
    ['907', 'ratio', 'mcr12_132', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '066'", 'M', 'MDCD_PD_AMT', "STC_CD= '066'"],
    ['907', 'ratio', 'mcr12_133', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '067'", 'M', 'MDCD_PD_AMT', "STC_CD= '067'"],
    ['907', 'ratio', 'mcr12_134', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '068'", 'M', 'MDCD_PD_AMT', "STC_CD= '068'"],
    ['907', 'ratio', 'mcr12_135', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '069'", 'M', 'MDCD_PD_AMT', "STC_CD= '069'"],
    ['907', 'ratio', 'mcr12_136', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '007'", 'M', 'MDCD_PD_AMT', "STC_CD= '007'"],
    ['907', 'ratio', 'mcr12_137', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '070'", 'M', 'MDCD_PD_AMT', "STC_CD= '070'"],
    ['907', 'ratio', 'mcr12_138', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '071'", 'M', 'MDCD_PD_AMT', "STC_CD= '071'"],
    ['907', 'ratio', 'mcr12_139', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '072'", 'M', 'MDCD_PD_AMT', "STC_CD= '072'"],

    ['907', 'ratio', 'mcr12_140', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '073'", 'M', 'MDCD_PD_AMT', "STC_CD= '073'"],
    ['907', 'ratio', 'mcr12_141', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '074'", 'M', 'MDCD_PD_AMT', "STC_CD= '074'"],
    ['907', 'ratio', 'mcr12_142', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '075'", 'M', 'MDCD_PD_AMT', "STC_CD= '075'"],
    ['907', 'ratio', 'mcr12_143', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '076'", 'M', 'MDCD_PD_AMT', "STC_CD= '076'"],
    ['907', 'ratio', 'mcr12_144', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '077'", 'M', 'MDCD_PD_AMT', "STC_CD= '077'"],
    ['907', 'ratio', 'mcr12_145', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '078'", 'M', 'MDCD_PD_AMT', "STC_CD= '078'"],
    ['907', 'ratio', 'mcr12_146', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '079'", 'M', 'MDCD_PD_AMT', "STC_CD= '079'"],
    ['907', 'ratio', 'mcr12_147', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '008'", 'M', 'MDCD_PD_AMT', "STC_CD= '008'"],
    ['907', 'ratio', 'mcr12_148', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '080'", 'M', 'MDCD_PD_AMT', "STC_CD= '080'"],
    ['907', 'ratio', 'mcr12_149', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '081'", 'M', 'MDCD_PD_AMT', "STC_CD= '081'"],

    ['907', 'ratio', 'mcr12_150', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '082'", 'M', 'MDCD_PD_AMT', "STC_CD= '082'"],
    ['907', 'ratio', 'mcr12_151', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '083'", 'M', 'MDCD_PD_AMT', "STC_CD= '083'"],
    ['907', 'ratio', 'mcr12_152', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '085'", 'M', 'MDCD_PD_AMT', "STC_CD= '085'"],
    ['907', 'ratio', 'mcr12_153', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '087'", 'M', 'MDCD_PD_AMT', "STC_CD= '087'"],
    ['907', 'ratio', 'mcr12_154', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '088'", 'M', 'MDCD_PD_AMT', "STC_CD= '088'"],
    ['907', 'ratio', 'mcr12_155', 'OT', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '089'", 'M', 'MDCD_PD_AMT', "STC_CD= '089'"],

    ['907', 'ratio', 'mcr18_9', 'RX', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '011'", 'M', 'MDCD_PD_AMT', "STC_CD= '011'"],
    ['907', 'ratio', 'mcr18_10', 'RX', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '127'", 'M', 'MDCD_PD_AMT', "STC_CD= '127'"],
    ['907', 'ratio', 'mcr18_11', 'RX', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '018'", 'M', 'MDCD_PD_AMT', "STC_CD= '018'"],
    ['907', 'ratio', 'mcr18_12', 'RX', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '033'", 'M', 'MDCD_PD_AMT', "STC_CD= '033'"],
    ['907', 'ratio', 'mcr18_13', 'RX', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '034'", 'M', 'MDCD_PD_AMT', "STC_CD= '034'"],
    ['907', 'ratio', 'mcr18_14', 'RX', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '036'", 'M', 'MDCD_PD_AMT', "STC_CD= '036'"],
    ['907', 'ratio', 'mcr18_15', 'RX', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '085'", 'M', 'MDCD_PD_AMT', "STC_CD= '085'"],
    ['907', 'ratio', 'mcr18_16', 'RX', 'CLL', 'Q', 'MDCD_FFS_EQUIV_AMT', "STC_CD= '089'", 'M', 'MDCD_PD_AMT', "STC_CD= '089'"]
]

df = DataFrame(run_907_all_ratio, columns=['series', 'cb', 'measure_id', 'claim_type', 'level', 'claim_cat1', 'var1', 'constraint1', 'claim_cat2', 'var2', 'constraint2'])
df['measure_id'] = df['measure_id'].str.upper()
# df = df.set_index("measure_id", drop = False)
print(df.head())
df.to_pickle('./run_907.pkl')

# CC0 1.0 Universal

# Statement of Purpose

# The laws of most jurisdictions throughout the world automatically confer
# exclusive Copyright and Related Rights (defined below) upon the creator and
# subsequent owner(s) (each and all, an "owner") of an original work of
# authorship and/or a database (each, a "Work").

# Certain owners wish to permanently relinquish those rights to a Work for the
# purpose of contributing to a commons of creative, cultural and scientific
# works ("Commons") that the public can reliably and without fear of later
# claims of infringement build upon, modify, incorporate in other works, reuse
# and redistribute as freely as possible in any form whatsoever and for any
# purposes, including without limitation commercial purposes. These owners may
# contribute to the Commons to promote the ideal of a free culture and the
# further production of creative, cultural and scientific works, or to gain
# reputation or greater distribution for their Work in part through the use and
# efforts of others.

# For these and/or other purposes and motivations, and without any expectation
# of additional consideration or compensation, the person associating CC0 with a
# Work (the "Affirmer"), to the extent that he or she is an owner of Copyright
# and Related Rights in the Work, voluntarily elects to apply CC0 to the Work
# and publicly distribute the Work under its terms, with knowledge of his or her
# Copyright and Related Rights in the Work and the meaning and intended legal
# effect of CC0 on those rights.

# 1. Copyright and Related Rights. A Work made available under CC0 may be
# protected by copyright and related or neighboring rights ("Copyright and
# Related Rights"). Copyright and Related Rights include, but are not limited
# to, the following:

#   i. the right to reproduce, adapt, distribute, perform, display, communicate,
#   and translate a Work;

#   ii. moral rights retained by the original author(s) and/or performer(s);

#   iii. publicity and privacy rights pertaining to a person's image or likeness
#   depicted in a Work;

#   iv. rights protecting against unfair competition in regards to a Work,
#   subject to the limitations in paragraph 4(a), below;

#   v. rights protecting the extraction, dissemination, use and reuse of data in
#   a Work;

#   vi. database rights (such as those arising under Directive 96/9/EC of the
#   European Parliament and of the Council of 11 March 1996 on the legal
#   protection of databases, and under any national implementation thereof,
#   including any amended or successor version of such directive); and

#   vii. other similar, equivalent or corresponding rights throughout the world
#   based on applicable law or treaty, and any national implementations thereof.

# 2. Waiver. To the greatest extent permitted by, but not in contravention of,
# applicable law, Affirmer hereby overtly, fully, permanently, irrevocably and
# unconditionally waives, abandons, and surrenders all of Affirmer's Copyright
# and Related Rights and associated claims and causes of action, whether now
# known or unknown (including existing as well as future claims and causes of
# action), in the Work (i) in all territories worldwide, (ii) for the maximum
# duration provided by applicable law or treaty (including future time
# extensions), (iii) in any current or future medium and for any number of
# copies, and (iv) for any purpose whatsoever, including without limitation
# commercial, advertising or promotional purposes (the "Waiver"). Affirmer makes
# the Waiver for the benefit of each member of the public at large and to the
# detriment of Affirmer's heirs and successors, fully intending that such Waiver
# shall not be subject to revocation, rescission, cancellation, termination, or
# any other legal or equitable action to disrupt the quiet enjoyment of the Work
# by the public as contemplated by Affirmer's express Statement of Purpose.

# 3. Public License Fallback. Should any part of the Waiver for any reason be
# judged legally invalid or ineffective under applicable law, then the Waiver
# shall be preserved to the maximum extent permitted taking into account
# Affirmer's express Statement of Purpose. In addition, to the extent the Waiver
# is so judged Affirmer hereby grants to each affected person a royalty-free,
# non transferable, non sublicensable, non exclusive, irrevocable and
# unconditional license to exercise Affirmer's Copyright and Related Rights in
# the Work (i) in all territories worldwide, (ii) for the maximum duration
# provided by applicable law or treaty (including future time extensions), (iii)
# in any current or future medium and for any number of copies, and (iv) for any
# purpose whatsoever, including without limitation commercial, advertising or
# promotional purposes (the "License"). The License shall be deemed effective as
# of the date CC0 was applied by Affirmer to the Work. Should any part of the
# License for any reason be judged legally invalid or ineffective under
# applicable law, such partial invalidity or ineffectiveness shall not
# invalidate the remainder of the License, and in such case Affirmer hereby
# affirms that he or she will not (i) exercise any of his or her remaining
# Copyright and Related Rights in the Work or (ii) assert any associated claims
# and causes of action with respect to the Work, in either case contrary to
# Affirmer's express Statement of Purpose.

# 4. Limitations and Disclaimers.

#   a. No trademark or patent rights held by Affirmer are waived, abandoned,
#   surrendered, licensed or otherwise affected by this document.

#   b. Affirmer offers the Work as-is and makes no representations or warranties
#   of any kind concerning the Work, express, implied, statutory or otherwise,
#   including without limitation warranties of title, merchantability, fitness
#   for a particular purpose, non infringement, or the absence of latent or
#   other defects, accuracy, or the present or absence of errors, whether or not
#   discoverable, all to the greatest extent permissible under applicable law.

#   c. Affirmer disclaims responsibility for clearing rights of other persons
#   that may apply to the Work or any use thereof, including without limitation
#   any person's Copyright and Related Rights in the Work. Further, Affirmer
#   disclaims responsibility for obtaining any necessary consents, permissions
#   or other rights required for any use of the Work.

#   d. Affirmer understands and acknowledges that Creative Commons is not a
#   party to this document and has no duty or obligation with respect to this
#   CC0 or use of the Work.

# For more information, please see
# <http://creativecommons.org/publicdomain/zero/1.0/>
