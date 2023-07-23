package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;
import java.util.stream.Collectors;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     * <p>
     * `requestType` is guaranteed to be one of: S, X, NL.
     * <p>
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     * lock type can be, and think about how ancestor looks will need to be
     * acquired or changed.
     * <p>
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // same lock type should return immediately
        if (requestType.equals(explicitLockType) || requestType.equals(effectiveLockType)) {
            // IX -> X -> NL(request X)
            // IS -> S -> NL(request S)
            // IX -> IX -> X(request X)
            // IS -> IS -> S(request S)
            // do nothing
            return;
        }

        if (lockContext.readonly) {
            return;
        }

        // The current lock type can effectively substitute the requested type
        if (requestType.equals(LockType.NL)) {
            lockContext.release(transaction);
        } else if (explicitLockType.isIntent()) {
            if (requestType.equals(LockType.S)) {
                if (explicitLockType.equals(LockType.IS)) {
                    lockContext.escalate(transaction);
                } else if (explicitLockType.equals(LockType.IX)) {
                    lockContext.promote(transaction, LockType.SIX);
                }
            } else {
                if (explicitLockType.equals(LockType.IS)) {
                    // IS -> IS(request X) -> S
                    // step 1: IX -> IS -> S, make parent to IX
                    LockUtil.ensureParentContext(lockContext.parent, requestType);
                    // step 2: IX -> S(request X), escalate children
                    lockContext.escalate(transaction);
                    // step 3: IX -> S(request X), promote current S to X
                    lockContext.promote(transaction, requestType);
                } else if (explicitLockType.equals(LockType.IX) || explicitLockType.equals(LockType.SIX)) {
                    // explicitLockType => SIX or IX
                    // requestType => X
                    lockContext.escalate(transaction);
                }
            }
        } else if (explicitLockType.equals(LockType.NL) && !LockType.compatible(effectiveLockType, requestType)) {
            // fix these two situations
            // IS -> S -> .... -> NL(request X)
            // IX -> X -> .... -> NL(request S)
            LockContext effectiveContext = LockUtil.getEffectiveParentContext(lockContext);
            // clean up all children
            // IS -> S -> .... -> NL(request X) -> NL -> NL -> .... -> NL(request X)
            // IX -> X -> .... -> NL(request S) -> NL -> NL -> .... -> NL(request S)
            LockUtil.ensureNoLockHeldOnChildren(effectiveContext);
            // ensure lock again
            // IS -> S -> .... -> NL(request X) -> IX -> IX -> .... -> X
            // IX -> X -> .... -> NL(request S) -> IS -> IS -> .... -> S
            LockUtil.ensureSufficientLockHeld(lockContext, requestType);
        } else {
            // effectiveLockType: NL / X / S
            // request: X / S
            // request != explicitLock
            // potential pairs:
            // 1. NL -> X
            // 2. NL -> S
            // 3. X -> S
            // 4. S -> X
            boolean canBeParentLock = parentContext == null ||
                    LockType.canBeParentLock(parentContext.getEffectiveLockType(transaction), requestType);

            if (!canBeParentLock) {
                LockUtil.ensureParentContext(lockContext.parent, requestType);
            }

            // Why use effectiveLockType?
            // If we use explicitLock, these cases will break the multi-granularity policy.
            // IS -> S -> NL(request X)
            //      ==> LockType.compatible(X, NL) = true ==> will run lockContext.acquire(transaction, X)
            //      and throws error, because we can't request X lock with S/IS parents.
            // IX -> X -> NL(request S)
            //      ==> LockType.compatible(S, NL) = true ==> will run lockContext.acquire(transaction, S)
            //      and throws error, because we can't request S lock with X/IX parents.

            // pair 1 / 2
            if (LockType.compatible(requestType, effectiveLockType)) {
                lockContext.acquire(transaction, requestType);
            }
            // 4
            else if (LockType.substitutable(requestType, effectiveLockType)) {
                lockContext.promote(transaction, requestType);
            }
            // 3
            // IX -> X(request S) -> NL
            // step 1 (canBeParentLock = true)
            // step 2 (In the same transaction, type X is a strict type S, they are the same lock, so we don't need to do any ops.)
        }
    }

    static public void ensureNoLockHeldOnChildren(LockContext root) {
        TransactionContext transaction = TransactionContext.getTransaction();
        List<LockContext> currentDBLocks = root.lockman.getLocks(transaction).stream()
                .map(l -> LockContext.fromResourceName(root.lockman, l.name))
                .collect(Collectors.toList());
        List<LockContext> sortedDBLockCtx = LockUtil.getOrderedContext(currentDBLocks, true);
        for (LockContext ctx : sortedDBLockCtx) {
            ctx.release(transaction);
        }
    }

    /**
     * @param releaseAction `true` will give a bottom-up order
     *                      `false` will give a top-down order for lock acquire/promote
     */
    static public List<LockContext> getOrderedContext(List<LockContext> contexts, boolean releaseAction) {
        contexts.sort(new Comparator<LockContext>() {
            @Override
            public int compare(LockContext o1, LockContext o2) {
                int size1 = o1.name.getNames().size();
                int size2 = o2.name.getNames().size();
                return releaseAction ? size2 - size1 : size1 - size2;
            }
        });

        return contexts;
    }


    static public LockContext getEffectiveParentContext(LockContext context) {
        LockContext currentLockContext = context;
        while (currentLockContext.parent != null) {
            currentLockContext = currentLockContext.parent;
        }

        return currentLockContext;
    }

    static public void ensureParentContext(LockContext parentContext, LockType childLock) {
        TransactionContext transaction = TransactionContext.getTransaction();
        List<LockContext> allParentContexts = new ArrayList<>();
        LockType parentLock = LockType.parentLock(childLock);

        LockContext currentLockContext = parentContext;
        while (currentLockContext != null) {
            allParentContexts.add(currentLockContext);
            currentLockContext = currentLockContext.parent;
        }

        List<LockContext> sortedParentContexts = LockUtil.getOrderedContext(allParentContexts, false);

        for (LockContext ctx : sortedParentContexts) {
            LockType type = ctx.getExplicitLockType(transaction);

            if (type.equals(parentLock)) {
                continue;
            }

            if (type.equals(LockType.NL)) {
                ctx.acquire(transaction, parentLock);
            } else if (LockType.substitutable(parentLock, type)) {
                ctx.promote(transaction, parentLock);
            }
        }
    }

    // TODO(proj4_part2) add any helper methods you want
}
