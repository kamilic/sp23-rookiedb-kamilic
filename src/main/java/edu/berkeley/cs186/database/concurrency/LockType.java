package edu.berkeley.cs186.database.concurrency;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if (a.equals(LockType.NL) || b.equals(LockType.NL)) {
            return true;
        }

        if (a.equals(LockType.IX) && b.equals(LockType.IX)) {
            return true;
        }

        if (a.equals(LockType.S) && b.equals(LockType.S)) {
            return true;
        }

        if ((a.equals(LockType.SIX) && b.equals(LockType.IS)) || (b.equals(LockType.SIX) && a.equals(LockType.IS))) {
            return true;
        }

        if ((a.equals(LockType.S) && b.equals(LockType.IS)) || (b.equals(LockType.S) && a.equals(LockType.IS))) {
            return true;
        }

        if ((a.isIntent() && !a.equals(LockType.SIX)) && (b.isIntent() && !b.equals(LockType.SIX))) {
            return true;
        }

        return false;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
            case S:
                return IS;
            case X:
                return IX;
            case IS:
                return IS;
            case IX:
                return IX;
            case SIX:
                return IX;
            case NL:
                return NL;
            default:
                throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if (childLockType.equals(LockType.NL)) {
            return true;
        }

        // IX can be the parent of any type of lock
        // S | IS can be IX
        // SIX parent must be IX
        // X | IX can be IX
        if (parentLockType.equals(LockType.IX)) {
            return true;
        }

        if (childLockType.equals(LockType.S) || childLockType.equals(LockType.IS)) {
            return parentLockType.equals(LockType.IS);
        }

        if (childLockType.equals(LockType.X) || childLockType.equals(LockType.IX)) {
            return parentLockType.equals(LockType.SIX);
        }

        return false;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if (required.equals(substitute)) {
            return true;
        }

        if (required.equals(LockType.NL)) {
            return true;
        }

        if ((required.equals(LockType.IS) && substitute.equals(LockType.IX))) {
            return true;
        }

        if ((required.equals(LockType.IX) && substitute.equals(LockType.SIX))) {
            return true;
        }

        // X can be considered as a strict S lock.
        if (required.equals(LockType.S) && (substitute.equals(LockType.SIX) || substitute.equals(LockType.X))) {
            return true;
        }

        return false;
    }


    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
            case S:
                return "S";
            case X:
                return "X";
            case IS:
                return "IS";
            case IX:
                return "IX";
            case SIX:
                return "SIX";
            case NL:
                return "NL";
            default:
                throw new UnsupportedOperationException("bad lock type");
        }
    }
}

