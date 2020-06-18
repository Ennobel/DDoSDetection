package extract

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf

object FeatureExtraction{
    val px = udf((origPkts: Int, respPkts: Int) => {
        var result = 0
        if(origPkts != 0 && respPkts != 0){
            result = origPkts + respPkts 
            result
        }else{
            result
        }
    })

    val nnp = udf((px: Int) => {
        var result = 0
        if( px == 0 ){
          result =  1;
        }else{
          result =  0;
        }

        result
    })

    val nsp = udf((px: Int) => {
        var result = 0

        if(px >= 63 && px <= 400 ){
            result = 1;
        }else{
            result = 0;
        }
        result
    })

    val psp = udf((nsp:Double, px: Double) => {
        var result = 0.0
        if(px == 0.0){
            result = 0.0
        }else{
            result = nsp / px;
        }
        result
    })

    val iopr = udf((origPkts:Int, respPkts:Int) => {
        var result = 0.0
        if(respPkts != 0){
            result = origPkts / respPkts;
        }else{
            result = 0.0
        }
        result
    })

    val reconnect = udf((history:String) => {
        var result = 0
        var temp = history.take(2)
        if (temp == "Sr"){
            result = 1
        }else{
            result = 0
        }
        result
    })

    val fps = udf((origIpBytes:Int, origPkts:Int) => {
        var result = 0
        if(origPkts !=0 ){
          result = origIpBytes / origPkts                    
        }else{
          result = 0
        }

        result
    })

    val tbt = udf((origIpBytes:Int, respIpBytes:Int) => origIpBytes + respIpBytes )

    val apl = udf((px:Int, origIpBytes:Int, respIpBytes:Int) => {
        var result = 0
        if(px == 0){
            result = 0             
        }else{
            result = (origIpBytes + respIpBytes )/px
        }
        result
    })
    
    val pps = udf((duration:Double, fps:Double) => {
        var result = 0.0
        if(fps != 0){            
            result = (fps / duration).asInstanceOf[Double]
        }else{
            result = 0.0
        }
        result
    })

    // val pps = udf((duration:Double) => {
    //     var result = duration
    //     result
    // })
}
