/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dainst.arachne;

/**
 *
 * @author Simon Hohl
 * @author Reimar Grabowski
 */
class ArachneEntity {

    public String arachneID;
    public boolean foreignKey;
    public String restrictingTable;

    public ArachneEntity(String arachneID, boolean useForeignKey, String restrictingTable) {
        this.arachneID = arachneID;
        this.foreignKey = useForeignKey;
        this.restrictingTable = restrictingTable;
    }
}
