use alloy::sol;

sol! {
    struct ExecutorCall {
        address target;
        uint256 value;
        bytes data;
    }

    #[sol(rpc)]
    interface IArbExecutor {
        function executeArb(bytes calldata packedRoute) external returns (uint256 realizedProfit);
        function executeArbDirect(bytes calldata packedRoute) external returns (uint256 realizedProfit);
        function executeArbWithAave(bytes calldata packedRoute) external returns (uint256 realizedProfit);
        function executeArbWithDodo(bytes calldata packedRoute) external returns (uint256 realizedProfit);
        function transferAll(address token, address to) external;
    }

    #[sol(rpc)]
    interface IERC20 {
        event Transfer(address indexed from, address indexed to, uint256 value);
        function transfer(address to, uint256 amount) external returns (bool);
        function approve(address spender, uint256 amount) external returns (bool);
    }

    #[sol(rpc)]
    interface IUniswapV2Pair {
        function swap(uint256 amount0Out, uint256 amount1Out, address to, bytes calldata data) external;
        function getReserves() external view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast);
        event Sync(uint112 reserve0, uint112 reserve1);
    }

    #[sol(rpc)]
    interface IUniswapV3Pool {
        function slot0() external view returns (
            uint160 sqrtPriceX96,
            int24 tick,
            uint16 observationIndex,
            uint16 observationCardinality,
            uint16 observationCardinalityNext,
            uint8 feeProtocol,
            bool unlocked
        );
        function liquidity() external view returns (uint128);
        function fee() external view returns (uint24);
        function swap(
            address recipient,
            bool zeroForOne,
            int256 amountSpecified,
            uint160 sqrtPriceLimitX96,
            bytes calldata data
        ) external returns (int256 amount0, int256 amount1);
        event Swap(
            address indexed sender,
            address indexed recipient,
            int256 amount0,
            int256 amount1,
            uint160 sqrtPriceX96,
            uint128 liquidity,
            int24 tick
        );
    }

    #[sol(rpc)]
    interface ICurveStablePool {
        function exchange(int128 i, int128 j, uint256 dx, uint256 min_dy) external returns (uint256);
    }

    #[sol(rpc)]
    interface ICurveCryptoPool {
        function exchange(uint256 i, uint256 j, uint256 dx, uint256 min_dy) external returns (uint256);
        function price_scale(uint256 i) external view returns (uint256);
        function precisions() external view returns (uint256[2]);
    }

    #[sol(rpc)]
    interface ICurveStableNgPool {
        function exchange(int128 i, int128 j, uint256 dx, uint256 min_dy, address receiver) external returns (uint256);
    }

    struct BalancerSingleSwap {
        bytes32 poolId;
        uint8 kind;
        address assetIn;
        address assetOut;
        uint256 amount;
        bytes userData;
    }

    struct BalancerFundManagement {
        address sender;
        bool fromInternalBalance;
        address recipient;
        bool toInternalBalance;
    }

    struct BalancerBatchSwapStep {
        bytes32 poolId;
        uint256 assetInIndex;
        uint256 assetOutIndex;
        uint256 amount;
        bytes userData;
    }

    /// Balancer V2 IFlashLoanRecipient — abis/balancer-v2/IFlashLoanRecipient.sol
    #[sol(rpc)]
    interface IFlashLoanRecipient {
        function receiveFlashLoan(
            address[] memory tokens,
            uint256[] memory amounts,
            uint256[] memory feeAmounts,
            bytes memory userData
        ) external;
    }

    #[sol(rpc)]
    interface IBalancerVault {
        function swap(
            BalancerSingleSwap memory singleSwap,
            BalancerFundManagement memory funds,
            uint256 limit,
            uint256 deadline
        ) external payable returns (uint256);

        function batchSwap(
            uint8 kind,
            BalancerBatchSwapStep[] memory swaps,
            address[] memory assets,
            BalancerFundManagement memory funds,
            int256[] memory limits,
            uint256 deadline
        ) external payable returns (int256[] memory assetDeltas);

        function queryBatchSwap(
            uint8 kind,
            BalancerBatchSwapStep[] memory swaps,
            address[] memory assets,
            BalancerFundManagement memory funds
        ) external returns (int256[] memory assetDeltas);

        /// Vault flash loan — `executeArb` calls this from ArbExecutor.huff (selector 0x5c38449e).
        function flashLoan(
            address recipient,
            address[] memory tokens,
            uint256[] memory amounts,
            bytes memory userData
        ) external;
    }

    #[sol(rpc)]
    interface IDodoPool {
        function sellBase(address to) external returns (uint256 receiveQuoteAmount);
        function sellQuote(address to) external returns (uint256 receiveBaseAmount);
    }

    #[sol(rpc)]
    interface IWoofiRouter {
        function swap(
            address fromToken,
            address toToken,
            uint256 fromAmount,
            uint256 minToAmount,
            address to,
            address rebateTo
        ) external payable returns (uint256 realToAmount);
    }

    struct V4PoolKey {
        address currency0;
        address currency1;
        uint24 fee;
        int24 tickSpacing;
        address hooks;
    }

    #[sol(rpc)]
    interface IUniswapV4PoolManager {
        function unlock(bytes calldata data) external payable returns (bytes memory result);
        function extsload(bytes32 slot) external view returns (bytes32);
    }

    #[sol(rpc)]
    interface IAlgebraPool {
        function globalState() external view returns (
            uint160 price,
            int24 tick,
            uint16 lastFee,
            uint8 pluginConfig,
            uint16 communityFee,
            bool unlocked
        );
        function tickTable(int16 wordPosition) external view returns (uint256);
        function ticks(int24 tick) external view returns (
            uint128 liquidityTotal,
            int128 liquidityDelta,
            uint256 outerFeeGrowth0Token,
            uint256 outerFeeGrowth1Token,
            int56 outerTickCumulative,
            uint160 outerSecondsPerLiquidity,
            uint32 outerSecondsSpent,
            bool initialized
        );
    }

    #[sol(rpc)]
    interface IAlgebraIntegralPool {
        function tickTable(int16 wordPosition) external view returns (uint256);
        function ticks(int24 tick) external view returns (
            uint256 liquidityTotal,
            int128 liquidityDelta,
            int24 prevTick,
            int24 nextTick,
            uint256 outerFeeGrowth0Token,
            uint256 outerFeeGrowth1Token
        );
    }

    #[sol(rpc)]
    interface IMulticall3 {
        struct Call3 {
            address target;
            bool allowFailure;
            bytes callData;
        }
        struct Result {
            bool success;
            bytes returnData;
        }
        function aggregate3(Call3[] calldata calls) external payable returns (Result[] memory returnData);
    }

    #[sol(rpc)]
    interface IDodoPoolState {
        function _I_() external view returns (uint256);
        function _K_() external view returns (uint256);
        function _BASE_RESERVE_() external view returns (uint256);
        function _QUOTE_RESERVE_() external view returns (uint256);
        function _LP_FEE_RATE_() external view returns (uint256);
        function _BASE_TOKEN_() external view returns (address);
        function _QUOTE_TOKEN_() external view returns (address);
        function getPMMStateForCall() external view returns (
            uint256 i, uint256 K, uint256 B, uint256 Q,
            uint256 B0, uint256 Q0, uint256 R
        );
    }

    /// DODO V2 flash loan interface — shared by DVM/DPP/DSP pools.
    #[sol(rpc)]
    interface IDodoFlashLoan {
        function flashLoan(uint256 baseAmount, uint256 quoteAmount, address assetTo, bytes calldata data) external;
        function _BASE_TOKEN_() external view returns (address);
        function _QUOTE_TOKEN_() external view returns (address);
    }

    #[sol(rpc)]
    interface ICurvePool {
        function A() external view returns (uint256);
        function fee() external view returns (uint256);
        function balances(uint256 i) external view returns (uint256);
        function gamma() external view returns (uint256);
        function price_scale() external view returns (uint256);
        function stored_rates() external view returns (uint256[]);
    }

    #[sol(rpc)]
    interface IBalancerPool {
        function getPoolId() external view returns (bytes32);
        function getSwapFeePercentage() external view returns (uint256);
        function getNormalizedWeights() external view returns (uint256[]);
        function getAmplificationParameter() external view returns (uint256 value, bool isUpdating, uint256 precision);
        function getScalingFactors() external view returns (uint256[]);
    }

    #[sol(rpc)]
    interface IBalancerLinearPool {
        function getMainToken() external view returns (address);
        function getWrappedToken() external view returns (address);
        function getTargets() external view returns (uint256 lowerTarget, uint256 upperTarget);
        function getWrappedTokenRate() external view returns (uint256);
    }

    #[sol(rpc)]
    interface IBalancerVaultRead {
        function getPoolTokens(bytes32 poolId) external view returns (
            address[] tokens,
            uint256[] balances,
            uint256 lastChangeBlock
        );
    }

    #[sol(rpc)]
    interface IWoofiPool {
        function quoteToken() external view returns (address);
        function wooracle() external view returns (address);
        function tokenInfos(address base) external view returns (
            uint256 reserve,
            uint256 feeRate,
            uint256 maxGamma,
            uint256 maxNotionalSwap,
            bool enabled
        );
    }

    #[sol(rpc)]
    interface IWooracle {
        function state(address base) external view returns (
            uint128 price,
            uint64 spread,
            uint64 coeff,
            bool woFeasible
        );
        function decimals(address base) external view returns (uint8);
    }

    #[sol(rpc)]
    interface IERC20Metadata {
        function decimals() external view returns (uint8);
        function balanceOf(address account) external view returns (uint256);
    }

    /// Aave V3 IFlashLoanSimpleReceiver — abis/aave-v3/IFlashLoanSimpleReceiver.sol
    #[sol(rpc)]
    interface IFlashLoanSimpleReceiver {
        function executeOperation(
            address asset,
            uint256 amount,
            uint256 premium,
            address initiator,
            bytes calldata params
        ) external returns (bool);
    }

    /// Aave V3 IPool flash-loan surface — abis/aave-v3/IPool.sol
    #[sol(rpc)]
    interface IAaveV3Pool {
        function FLASHLOAN_PREMIUM_TOTAL() external view returns (uint128);
        function getReserveData(address asset) external view returns (
            uint256 configuration,
            uint128 liquidityIndex,
            uint128 currentLiquidityRate,
            uint128 variableBorrowIndex,
            uint128 currentVariableBorrowRate,
            uint128 currentStableBorrowRate,
            uint40 lastUpdateTimestamp,
            uint16 id,
            address aTokenAddress,
            address stableDebtTokenAddress,
            address variableDebtTokenAddress,
            address interestRateStrategyAddress,
            uint128 accruedToTreasury,
            uint128 unbacked,
            uint128 isolationModeTotalDebt
        );
        /// `executeArbWithAave` calls this from ArbExecutor.huff (selector 0x42b0b77c).
        function flashLoanSimple(
            address receiverAddress,
            address asset,
            uint256 amount,
            bytes calldata params,
            uint16 referralCode
        ) external;
    }

    #[sol(rpc)]
    interface ITickLens {
        struct PopulatedTick {
            int24 tick;
            int128 liquidityNet;
            uint128 liquidityGross;
        }
        function getPopulatedTicksInWord(address pool, int16 tickBitmapIndex)
            external
            view
            returns (PopulatedTick[] memory populatedTicks);
    }

    #[sol(rpc)]
    interface IChainlinkAggregator {
        function latestRoundData() external view returns (
            uint80 roundId,
            int256 answer,
            uint256 startedAt,
            uint256 updatedAt,
            uint80 answeredInRound
        );
    }

}
