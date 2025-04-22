/* pseudocode for corfu storage 

class CorfuStorage {

    private int s_epoch = 0      // initially 0, used to tell client if their mapping is out of date
    private Map<int, int> address_map = new HashMap()     // for mapping virtual to physical addresses
    private int mark = 0      // before this address, there are no unwritten addresses (updated in write, used for seal)

    public int write {
        
    }

    public byte[] read {

    }

    public int delete {
        
    }

    public int seal {
        
    }
}

*/