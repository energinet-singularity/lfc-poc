# Timout for poll
TIMEOUT_MS_POLL = 100

# refresh rates
REFRESH_RATE_MS_LFC_INPUT = 100
REFRESH_RATE_S_LFC_DEM_SIMU = 1

# decimal precision
PRECISION_DECIMALS = 2

# lfc cycle time
CYCLETIME_S_LFC = 4

# LFC target value (always zero)
SETPOINT_LFC_P_INPUT = 0

# LFC controller settings (PID, deadband for controller to force error to zero)
KP = 0.008
KI = 0.014
KD = 0
DEADBAND_LFC_ERROR = 0.5

# pbr responce simu settings
PBR_RAMP_MWM = 30
PBR_RAMP_MWS = PBR_RAMP_MWM/60
DEADBAND_PBR_SIMU = PBR_RAMP_MWS*REFRESH_RATE_S_LFC_DEM_SIMU
