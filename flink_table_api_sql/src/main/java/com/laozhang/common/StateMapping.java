package com.laozhang.common;

public class StateMapping {

//	保养报警集合
//	private static Set<String> maintainAlarmSet = Collections.unmodifiableSet(new HashSet<>());
//
//	public static boolean isMaintainAlarm(String alarmNo) {
//		return maintainAlarmSet.contains(alarmNo);
//	}
	
	/**
	 * 状态说明：1-运行，2-待料/上下料，3-报警，4-停机，5-调机，
	 * 6-保养，7-换刀报警，8-首件检测报警，9-暖机，10-自定义
	 */
	public static int getState(String alarmNo,String alarmType,String alarmMsg,
							   String mainPro,String subPro,
							   String run,String aut,
							   int oldState){
		//程式第一个是大写的字母“O”,第二个是数字0
		if(!alarmNo.startsWith("SV") && ("O0123".equals(mainPro) || "O0123".equals(subPro))){
			return 9;
		}

//    	if(isMaintainAlarm(alermNo)) {//只要包含保养报警，则状态为6
//    		return 6;
//    	}
		
		if("ALarM".equals(alarmType)){//当前记录为ALarM时为报警状态
			int stateNum;
			if(alarmMsg.toLowerCase().contains("dao ju shou ming dao") || alarmMsg.toLowerCase().contains("qing que ren huan da") || alarmMsg.contains("TOOLLIFE DAO QI")) {//换刀报警
				stateNum = 7;
			}else if(alarmMsg.contains("NEW TOOL")) {//首件检测
				stateNum = 8;
			}else {//普通报警
				stateNum = 3;
			}
			return stateNum;
		}else if(!"MEMory".equals(aut)){//调机
			return 5;
		}
		
    	switch(run) {
    		case "STaRT"://运行
    			return 1;
    		case "****(reset)":
				if(3 == oldState) {
					return 3;
				}else {
					return 2;
				}
    		case "STOP":
    			return 2;
    		default:
    			return 1;
    	}
    }
}
