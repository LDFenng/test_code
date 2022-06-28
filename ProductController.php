<?php

namespace App\Http\Controllers\Api\Wechat;

use App\Constants\Common;
use App\Models\Basic\MmsCategory;
use App\Models\Basic\MonitorProduct;
use App\Models\Es\PddBillboardGoodsRank;
use App\Models\Es\PddGoods;
use App\Models\Es\PddGoodsRankCategory;
use App\Models\Es\PddGoodsSimple;
use App\Models\Es\ProductSale;
use App\Models\Es\SpuRank;
use App\Services\AttributeService;
use App\Services\GoodsService;
use App\Services\MallService;
use App\Services\MmsPddService;
use App\Services\ProductAndMallDetail\ProductDetailService;
use App\Services\PublicGatewayService;
use App\Services\UserClassAccessService;
use App\Services\UserInfoService;
use App\Services\Wechat\KeywordHandle;
use App\Services\Wechat\TitleAnalysis;
use Carbon\Carbon;
use Illuminate\Http\Request;

ini_set('max_execution_time', 600);
class ProductController extends BaseController
{
    private $pika = '';
    private $array = [
        1 => '畅销榜',
        2 => '好评榜',
        3 => '新品榜',
        4 => '降价榜',
    ];

    public function __construct(Request $request)
    {
        parent::__construct($request);
        $this->pika = app('redis')->connection('dianba_search_pika');
    }

    /**
     * 首页热销商品(仅展示5个)
     */
    public function product(Request $request)
    {
        $params = $request->all();
        $data = [];
        $goodsIds = [];
        $keyword = addslashes(strip_tags($params['keyword'] ?? ''));
        $date = Carbon::today()->toDateString();
        if (Carbon::today()->diffInHours(Carbon::now()) < Common::ANALYST_STATIC_DELAY) {
            $date = Carbon::yesterday()->toDateString();
        }
        $catInfo = [
            'catid' => $params['catid_3'] ?? ($params['catid_2'] ?? $params['catid_1'] ?? $params['catid']??0),
            'level' => isset($params['catid_3']) ? 3 : (isset($params['catid_2']) ? 2 : (isset($params['catid_1'])?1:0))
        ]; 
        $key = 'mini_wechat_index_eg_data';
        $list = $this->pika->get($key);
        if($list){
            return $this->successResponse(json_decode($list,true));
        }
        $goodsIds = [];
        $goodsQuery = '';
        if ($keyword) {
            $goodsQuery = PddGoodsSimple::where('goods_name', 'like', $keyword);
        } else {
            $goodsIds = $request->payload['eg_goods_ids'];
            // $goodsQuery = PddGoodsSimple::whereIn('goods_id', array_rand(array_flip(Vip::EG_DATA['goods_ids']), 5));
        }
        if (!empty($catInfo['catid'])) {
            if (empty($goodsQuery)) {
                $goodsQuery = PddGoodsSimple::where('cat_id_' . $catInfo['level'], $catInfo['catid']);
            } else {
                $goodsQuery = $goodsQuery->where('cat_id_' . $catInfo['level'], $catInfo['catid']);
            }
        }
        if (!empty($goodsQuery)) {
            $goodsIds = $goodsQuery->orderBy('update_time', 'DESC')->take(1000)->get()->pluck('goods_id')->toArray();
        }
        if (empty($goodsIds) && !empty($keyword)) {
            return $this->successResponse();
        }
        $params['is_filter'] = false;
        $data = $this->getProducts($goodsIds, $date, $params, 5);

        $list = array_slice(arraySequence($data, 'day_sale'), 0, 5);
        if($list){
            $this->pika->set($key, json_encode($list, 320), 'EX', 3600);
        }
        return $this->successResponse(empty($data) ? [] : array_slice(arraySequence($data, 'day_sale'), 0, 5));
    }

    /**
     * 前台类目（默认主级）
     */
    public function getCategory(Request $request)
    {
        $level = $request->level ?? 0;
        $parent_id   = $request->pid ?? 0;
        $auth = $request->payload['auths']['level'] ?? 1;
        $classConfig = $request->payload['class_config'] ?? [];
        // $classService = $request->payload['class_service']??0;
        $auth = $request->payload['auths']['level'] ?? 1;

        if (empty($level) && empty($parent_id)) {
            //主营类目
            $items = (new KeywordHandle)->getMainCategoryDatas([], true);
            $list = [];
            foreach ($items as $val) {
                if (!empty($classConfig) && in_array($val['id'], $classConfig)) {
                    $list[] = [
                        'name' => $val['name'],
                        'cat_id' => $val['id']
                    ];
                }
            }
        } else {
            if ($level == 1) {
                $items = (new KeywordHandle)->getMainCategoryDatas([], true);
                $catIds = $items[$parent_id]['bind_admin_class'] ?? [];
            } else {
                $catIds = [$parent_id];
            }
            $items = (new KeywordHandle)->getCategoryDatas($catIds, $level);
            foreach ($items as $val) {
                $list[] = [
                    'name' => $val['cat_name'],
                    'cat_id' => $val['cat_id_' . $level]
                ];
            }
        }
        if ($auth <= 3) { //等级低于3时无类目
            $list = [];
        }

        return $this->successResponse($list ?: []);
    }

    /**
     * 后台类目（默认主级）
     */
    public function getAdminCategory(Request $request)
    {
        $level = $request->level;
        $pid   = $request->pid;
        $uid = $request->payload['uid'] ?? 0;
        $list = [];
        if ($level == 0 && $pid == 0) {
            $data = UserInfoService::getUserVipConfig($uid);
            foreach ($data as $val) {
                unset($val['bind_admin_class']);
                $list[] = $val;
            }
        } elseif ($level == 1) {
            $userClassAccessService = new UserClassAccessService();
            $data      = $userClassAccessService->getmmsFirstCategory($pid);
            foreach ($data as $val) {
                $list[] = [
                    'id' => $val['cat_id_1'],
                    'name' => $val['cat_name']
                ];
            }
        } else {
            $mmsPddService  = new MmsPddService();
            $data = $this->successResponse($mmsPddService->getMmsCategoriesLocal($level, $pid));
            foreach ($data as $val) {
                if (is_array($val)) {
                    foreach ($val['data'] as $vv) {
                        $list[] = [
                            'id' => $vv['id'],
                            'name' => $vv['cat_name']
                        ];
                    }
                }
            }
        }
        return $this->successResponse($list);
    }

    /**
     * 热销/飙升产品榜
     */
    public function hotProducts(Request $request)
    {
        $params = $request->all();

        $auths = $request->payload['auths']['goods_list'] ?? 5;
        $page = $params['page'] ?? 1;
        $pageSize = $params['page_size'] ?? 15;
        $key = md5(serialize($params) . $page . $pageSize . $auths) . ':v3';
        $data = $this->pika->get($key);
        if ($data) {
            return $this->successResponse(json_decode($data), true);
        }
        $goodsIds = [];
        $keyword = addslashes(strip_tags($params['keyword'] ?? ''));
        $date = Carbon::today()->toDateString();
        if (Carbon::today()->diffInHours(Carbon::now()) < Common::ANALYST_STATIC_DELAY) {
            $date = Carbon::yesterday()->toDateString();
        }
        $catInfo = [
            'catid' => $params['catid_3'] ?? ($params['catid_2'] ?? $params['catid_1'] ?? $params['catid']??0),
            'level' => isset($params['catid_3']) ? 3 : (isset($params['catid_2']) ? 2 : (isset($params['catid_1'])?1:0))
        ]; 
        $params['cat_info'] = $catInfo;
        $goodsQuery = '';
        if ($keyword) {
            $goodsQuery = PddGoods::where('goods_name', 'like', $keyword);
        }
        if ($auths == 'eg') {
            $params['is_filter'] = false;
            $goodsQuery = PddGoods::whereIn('goods_id', $request->payload['eg_goods_ids']);
            $auths = 5;
            if ($page > 1) {
                return $this->successResponse([]);
            }
        }
        if(($page-1)*$pageSize > $auths){
            return $this->successResponse([]);
        }
        if (!empty($goodsQuery)) {
            $goodsIds = $goodsQuery->orderBy('total_sales', 'DESC')->orderBy('last_update_time', 'DESC')
                ->take($auths * 10)->get()->pluck('goods_id')->toArray();
        }
        $data = $this->getProducts($goodsIds, $date, $params, $auths);
        if (empty($data)) {
            return $this->successResponse(['list'=>[]]);
        }
        $total = count($data);
        $list = arraySequence($data, ($params['type'] ?? 1 == 1 ? 'day_sale' : 'week_sale'));
        $items = [
            'list' => array_slice($list, ($page - 1) * $pageSize, $pageSize),
            'total' => $total,
            'total_page' => ceil($total / $pageSize)
        ];
        $this->pika->set($key, json_encode($items, 320), 'EX', 1800);
        return $this->successResponse($items);
    }


    /**
     * 属性排行榜类目,2级分类
     * @author Damow
     * @param mixed $data 参数一的说明
     * @return array
     */
    public function rankCategory()
    {
        //新增缓存,2021年3月22日15:59:41
        $pika = app('redis')->connection('pdd_cache');
        //新增日期切换
        $date = date('Y-m-d', strtotime('-2 days'));
        $switch = $this->pika->get('pdd_web:attribute:date');
        if (!empty($switch)) {
            $date = $switch;
        }
        $list1 = $pika->get(\App\Constants\Cache\TotalKey::RANK_LIST['duoduo']['category'] . $date);
        if (!empty($list1)) {
            return $this->successResponse(json_decode($list1, true));
        }

        $list1 = PddGoodsRankCategory::where('level', '1')->take(100)->where('date', $date)->orderBy('category_1', 'asc')->get()->toArray(); //1级分类
        if (empty($list1)) {
            $date = date('Y-m-d', strtotime('-2 days'));
            $list1 = PddGoodsRankCategory::where('level', '1')->take(100)->where('date', $date)->orderBy('category_1', 'asc')->get()->toArray(); //1级分类
        }
        $list2 = PddGoodsRankCategory::where('level', '2')->take(500)->where('date', $date)->orderBy('category_2', 'asc')->get()->groupBy('category_1')->toArray(); //2级分类
        foreach ($list1 as $k => $item) {
            unset($list1[$k]['_index']);
            unset($list1[$k]['_type']);
            unset($list1[$k]['_id']);
            unset($list1[$k]['_score']);
            unset($list1[$k]['_score']);
            $list1[$k]['child_list'] = $list2[$item['category_1']] ?? [];
            if (!empty($list1[$k]['child_list'])) {
                foreach ($list1[$k]['child_list'] as $kk => $vv) {
                    unset($list1[$k]['child_list'][$kk]['_index']);
                    unset($list1[$k]['child_list'][$kk]['_type']);
                    unset($list1[$k]['child_list'][$kk]['_id']);
                    unset($list1[$k]['child_list'][$kk]['_score']);
                    unset($list1[$k]['child_list'][$kk]['_score']);
                }
            }
        }
        if (!empty($list1)) {
            $pika->set(\App\Constants\Cache\TotalKey::RANK_LIST['duoduo']['category'] . $date, json_encode($list1));
            $pika->expire(\App\Constants\Cache\TotalKey::RANK_LIST['duoduo']['category'] . $date, 3600 * 25);
        }
        return $this->successResponse($list1);
    }

    /**
     * 类目榜单
     */
    public function categoryRank(Request $request)
    {
        $this->validateApiRequest($request, [
            'category_1' => 'required|numeric',
            'category_2' => 'required|numeric',
        ], [
            'category_1.required' => '1级分类不能为空',
            'category_1.numeric'  => '字段类型有误',
            'category_2.required' => '2级分类不能为空',
            'category_2.numeric'  => '字段类型有误',
        ]);
        //新增日期切换
        $switch = $this->pika->get('pdd_web:attribute:date');
        if (!empty($switch)) {
            $request->start_time = $switch;
            $request->end_time = $switch;
        }
        if (!$request->start_time) {
            $request->start_time = date('Y-m-d', strtotime('-1 days'));
        }
        if (!$request->end_time) {
            $request->end_time = date('Y-m-d', strtotime('-1 days'));
        }
        //新增缓存
        if (!isset($request->category_name) || empty($request->category_name)) {
            $pika = app('redis')->connection('pdd_cache');
            $keys = \App\Constants\Cache\TotalKey::RANK_LIST['duoduo']['category_rank'] . $request->start_time;
            $list = $pika->hget($keys, $request->category_2);
            $list = json_decode($list);
            if (!empty($list)) {
                $items = [
                    'date' => date('Y-m-d', strtotime('-1 days')),
                    'list' => $list
                ];
                return $this->successResponse($items);
            }
        }
        $date = date('Y-m-d', strtotime('-1 days'));
        $query = PddGoodsRankCategory::where('level', '3')->where('category_1', $request->category_1)->where('category_2', $request->category_2)->take(2000);
        if (isset($request->category_name) && !empty($request->category_name)) {
            $query = $query->where('category_name_3', 'like', $request->category_name);
        }
        if (isset($request->start_time) && !empty($request->start_time)) {
            $query = $query->where('date', '>=', $request->start_time);
        }
        if (isset($request->end_time) && !empty($request->end_time)) {
            $query = $query->where('date', '<=', $request->end_time);
        }
        if (!isset($request->start_time) && !isset($request->end_time)) {
            $query = $query->where('date', $date);
        }
        $result = [];
        $list = $query->get()->toArray(); //3级分类
        //2021年3月25日12:29:46 隐藏没有数据的类目
        $pika = app('redis')->connection('pdd_cache');
        $key  = \App\Constants\Cache\TotalKey::RANK_LIST['duoduo']['lists'] . $request->end_time;
        $key1  = \App\Constants\Cache\TotalKey::RANK_LIST['duoduo']['lists'] . $request->start_time;

        foreach ($list as $k => $item) {
            $res = $pika->hget($key, $item['category_3']);
            $res1 = $pika->hget($key1, $item['category_3']);
            if ((empty($res) || strlen($res) <= 3) && (empty($res1) || strlen($res1) <= 3)) {
                continue;
            }

            unset($list[$k]['_index'], $list[$k]['_type'], $list[$k]['_id'], $list[$k]['_score'], $list[$k]['_score']);
            !isset($result[$item['type']]['type']) && $result[$item['type']]['type'] = $item['type'];
            !isset($result[$item['type']]['name']) && $result[$item['type']]['name'] = $this->array[$item['type']];
            $result[$item['type']]['list'][] = $item;
        }
        $result = array_values($result);
        //数据过期
        if (!isset($request->category_name) || empty($request->category_name)) {
            $hour = intval(date('H'));
            //三点后才生成缓存，三点前还有缓存数据在跑
            if ($hour > 3) {
                $pika->hset($keys, $request->category_2, json_encode($result));
                $pika->expire($keys, 3600 * 2);
            }
        }
        $items = [
            'date' => date('Y-m-d', strtotime('-1 days')),
            'list' => $result
        ];
        return $this->successResponse($items);
    }

    /**
     * 榜单列表-多多排行榜
     * @author Damow
     * @param mixed $data 参数一的说明
     * @return array
     */
    public function pddRanklist(Request $request)
    {
        $this->validateApiRequest($request, [
            'category_3' => 'required|numeric',
            'type' => 'required|numeric',
        ], [
            'category_3.required' => '1级分类不能为空',
            'category_3.numeric'  => '字段类型有误',
            'type.required' => '榜单类型不能为空',
            'type.numeric'  => '榜单类型有误',
        ]);
        $params = $request->all();
        //新增日期切换
        $switch = $this->pika->get('pdd_web:attribute:date');
        if (!empty($switch)) {
            $request->start_time = $switch;
            $request->end_time = $switch;
        }
        $auths = $request->payload['auths']['goods_list'] ?? 5;
        $key  = 'pdd_web:attribute:lists:' . ($request->start_time ?: '') . md5(serialize($params)) . $auths;
        $list = $this->pika->get($key);
        if (!empty($list)) {
            return $this->successResponse(json_decode($list, true));
        }
        $date = date('Y-m-d', strtotime('-1 days'));
        $query = PddBillboardGoodsRank::where('billboard_id', $request->category_3)->where('list_type', $request->type);

        if ($auths == 'eg') {
            $auths = 5;
            $query = $query->whereIn('goods_id', $request->payload['eg_goods_ids']);
        } else {
            if (isset($request->goods_id) && !empty($request->goods_id)) {
                $query = $query->where('goods_id', $request->goods_id);
            }
            if (isset($request->start_time) && !empty($request->start_time)) {
                $query = $query->where('date', '>=', $request->start_time);
            }
            if (isset($request->end_time) && !empty($request->end_time)) {
                $query = $query->where('date', '<=', $request->end_time);
            }
            if (empty($request->start_time) && empty($request->end_time)) {
                $query = $query->where('date', $date);
            }
        }
        $list = $query->orderBy('rank', 'asc')->take($auths <= 200 ? $auths : 200)->get(); //3级分类
        //数据组装
        $AttributeService = new AttributeService();
        $list = $AttributeService->checkoutData($list->toArray());
        $this->pika->set($key, json_encode($list, 320), 'EX', 3600);
        return $this->successResponse($list);
    }

    /**
     * 产品详情
     */
    public function productDetail(Request $request)
    {
        $params = $request->all();
        $uid = $request->payload['uid'] ?? '';
        $this->validateApiRequest($request, [
            'goods_id' => 'required',
        ], [
            'goods_id.required' => '产品ID不可为空！'
        ]);
        $this->pika->zincrby('search_pdd:goods_info:zadd', 1, $params['goods_id']);

        $auths = $request->payload['auths']['detail'] ?? 'eg';
        if ($auths == 'eg') {
            $params['goods_id'] = $request->payload['eg_goods_ids'][0];
        }

        $key = 'pdd:wecaht_mini:product_detail:v2' . $params['goods_id'];
        $data = $this->pika->get($key);
        if ($data) {
            return $this->successResponse(json_decode($data, true));
        }
        $data = [];
        try {
            $data  = (new ProductDetailService)->detail($params['goods_id']);
        } catch (\Throwable $th) {
            return $this->successResponse();
        }
        $date = Carbon::today()->toDateString();
        if (Carbon::today()->diffInHours(Carbon::now()) < Common::ANALYST_STATIC_DELAY) {
            $date = Carbon::yesterday()->toDateString();
        }
        $extData = $this->getProducts([$params['goods_id']], $date, ['type' => 1], 1);
        if ($extData) {
            $data['goods_sale']['day_sale'] = $extData[0]['day_sale'];
            $data['goods_sale']['week_sale'] = $extData[0]['week_sale'];
            $data['goods_sale']['total_sale'] = $extData[0]['total_sales'];
        }
        //判断是否已监控
        $check = false;
        if ($uid) {
            $check = MonitorProduct::where('uid', $uid)->where('monitor_id', $params['goods_id'])->value('id');
        }

        $data['is_monitor'] = $check ? 1 : 0;
        $catInfo = [];
        if ($data['category_1']) {
            $catInfo = MmsCategory::getCategoryDatas([$data['category_1']], 1);
        }
        $data['catgory_name'] = $catInfo[$data['category_1']]['cat_name'] ?? '';
        $data['main_catgory_name'] = (new KeywordHandle)->getMainCategoryNames($data['category_1']);
        unset($data['_index'], $data['_id']);
        $this->pika->set($key, json_encode($data, 320), 'EX', strtotime(date('Y-m-d 09:00:00',strtotime('+1 days'))) - time());
        return  $this->successResponse($data);
    }

    /**
     * 数据趋势
     */
    public function productSale(Request $request)
    {
        $params = $request->all();
        $this->validateApiRequest($request, [
            'goods_id' => 'required',
        ], [
            'goods_id.required' => '产品ID不可为空！'
        ]);
        $auths = $request->payload['auths']['detail'] ?? 'eg';
        if ($auths == 'eg') {
            $params['goods_id'] = $request->payload['eg_goods_ids'][0];
        }
        $date_show = date('Y-m-d', strtotime('-8 days'));
        if (Carbon::today()->diffInHours(Carbon::now()) < Common::ANALYST_STATIC_DELAY) {
            $date_show = Carbon::yesterday()->subDay(8)->toDateString();
        }
        $data = ProductSale::select(['date', 'avg_price', 'sales', 'total_sales', 'amount'])
            ->where('goods_id', $params['goods_id'])
            ->where('date', '>=', $date_show)
            ->take(100)->orderBy('date', 'ASC')->get()->toArray();
        if (empty($data)) {
            return $this->successResponse();
        }
        $diffDays = round((time() - strtotime($date_show)) / 86400);
        $items = [];
        foreach ($data as $val) {
            $items[date('Y-m-d', strtotime($val['date']))] = $val;
        }
        $sales = [];
        $amounts = [];
        $dates = [];
        $dateSales = [];

        for ($i = $diffDays; $i > 0; $i--) {
            $date = date('Y-m-d', strtotime('-' . $i . ' days'));
            $subDate = date('Y-m-d', strtotime($date, strtotime('-1 days')));
            $dateSales[$subDate]['sale'] = $items[$date]['sales'] ?? 0;
            $dateSales[$subDate]['amount'] = round($items[$date]['amount'] ?? 0, 2);
            $dateSales[$subDate]['sale_amount'] = round(($dateSales[$subDate]['sale'] ?? 0) * ($dateSales[$subDate]['amount'] ?? 0), 2);
            $diff = ($dateSales[$subDate]['sale'] ?? 0) - ($dateSales[date('Y-m-d', strtotime('-' . ($i + 1) . ' days'))]['sale'] ?? 0);
            $dateSales[$subDate]['rate'] = $dateSales[$subDate]['sale'] <= 0 ? 0 : round($diff / $dateSales[$subDate]['sale'], 4) * 100;
            if ($i < $diffDays - 1) {
                if (isset($items[$date])) {
                    //再往前减一天。骚逼写法，别问我
                    $sales[$subDate] = $items[$date]['sales'];
                    $amounts[$subDate] = $items[$date]['amount'];
                } else {
                    $sales[$subDate] = 0;
                    $amounts[$subDate] = 0;
                }
                $dates[] = $subDate;
            }
        }
        $data = [
            'sales' => $sales, //array_values($sales),
            'amounts' => array_values($amounts),
            'date_sales' => $dateSales,
            'date' => $dates,
            'goods_id' => $params['goods_id']
        ];
        return $this->successResponse($data);
    }

    /**
     * 评论列表
     * @author Damow
     * @param mixed $data 参数一的说明
     * @return array
     */
    public function commentList(Request $request)
    {
        $this->validateApiRequest($request, [
            'goods_id'   => 'required|numeric',
            'sort'       => 'numeric',
            'excel'      => 'numeric',
            'lable_id'   => 'numeric',
        ], [
            'goods_id.numeric'    => '字段类型有误',
            'goods_id.required' => '产品ID不可为空！',
            'sort.numeric'        => '字段类型有误',
            'excel.numeric'       => '字段类型有误',
            'lable_id.numeric'       => '字段类型有误',
        ]);

        $goods_id = $request->goods_id ?? '';
        $auths = $request->payload['auths']['comment'] ?? 'eg';
        if ($auths == 'eg') {
            $request->goods_id = $request->payload['eg_goods_ids'][0];
        }
        if ($goods_id) {
            $this->pika->hset('pdd_comment_goods_ids', $goods_id, date("Y-m-d H:i:s"));
        }
        $page = $request->page;

        $key = 'pdd:wecaht_mini:commentList:' . $goods_id . $page;
        $data = $this->pika->get($key);
        if ($data) {
            return $this->successResponse(json_decode($data, true));
        }
        for ($i = 0; $i < 3; $i++) {
            $commentList = (new PublicGatewayService())->getDetailAppCommentList($goods_id, $page, $request->lable_id ?? 0);
            if ($commentList)
                break;
        }
        $type = $request->type ?? 0; //判断是否图片或者视频 。1图片；2视频
        $list = [];
        if ($commentList) {
            $commentData = $commentList["data"];
            foreach ($commentData as $k => $item) {
                $images = $video = [];
                !empty($item['pictures']) && $images = array_column($item['pictures'], 'url');
                !empty($item['video']) && $video = array($item['video']['url']);
                $list[$k]["review_id"] = $item['review_id'];
                $list[$k]["goods_id"] = $goods_id;
                $list[$k]["comprehensive_dsr"] = isset($item['comprehensive_dsr']) ? $item['comprehensive_dsr'] : $item['service_score'];
                $list[$k]["labels"] = $item['labels'] ?? '';
                $list[$k]["favor_count"] = $item['favor_count'];
                $list[$k]["reply_count"] = isset($item['reply_count']) ? $item['reply_count'] : 0;
                $list[$k]["user_avatar"] = $item['avatar'];
                $list[$k]["user_name"] = $item['name'];
                $list[$k]["comment"] = $item['comment'];
                $list[$k]["specs"] = json_decode($item['specs'], 1);
                $list[$k]["images"] = $images;
                $list[$k]["add_date"] = date('Y-m-d H:i:s', $item['time']);
                $list[$k]["video"] = $video;
                if ($type == 1 && empty($list[$k]["images"])) {
                    unset($list[$k]);
                } elseif ($type == 2 && empty($list[$k]["video"])) {
                    unset($list[$k]);
                }
            }
        }
        $this->pika->set($key, json_encode(array_values($list), 320), 'EX', 3600 * 12);
        return $this->successResponse(array_values($list));
    }

    /**
     * 产品里程碑
     */
    public function productMilepost(Request $request, ProductDetailService $detailService)
    {
        $params = $request->all();
        $this->validateApiRequest($request, [
            'goods_id' => 'required',
        ], [
            'goods_id.required' => '产品ID不可为空！'
        ]);
        $goods_id = $params['goods_id'];
        $auths = $request->payload['auths']['detail'] ?? 'eg';
        if ($auths == 'eg') {
            $goods_id = $request->payload['eg_goods_ids'][0];
        }
        $data = [];
        #发布状态 $res
        $res    = $detailService->getRelease($goods_id);
        #价格变化
        $price  = $detailService->changePrice($goods_id);
        #排名变化
        $rank   = $detailService->changeRank($goods_id);
        #上下架状态 $shelf
        $shelf  = $detailService->getShelf($goods_id);
        #多多排行榜数据变化
        $duoduo  = $detailService->changeDuoduo($goods_id);
        !empty($res)  && $data[] = $res;
        //多次记录则遍历追加
        $array = [$price, $rank, $shelf, $duoduo];
        foreach ($array as $arr) {
            $data = array_merge($data, $arr ?: []);
        }
        return $this->successResponse(array_filter($data));
    }

    /**
     * 标题诊断
     */
    public function diagnosisTitle(Request $request)
    {
        $params = $request->all();
        $this->validateApiRequest($request, [
            'goods_id' => 'required|numeric',
        ], [
            'goods_id.required' => '请填写goods_id',
            'goods_id.numeric' => '商品ID不是纯数字',
        ]);
        $goods_id = $request->goods_id;
        $auths = $request->payload['auths']['word_diagnosis'] ?? 'eg';
        if ($auths == 'eg') {
            $goods_id = $request->payload['eg_goods_ids'][0];
            $params['goods_id'] = $goods_id;
        }
        $params['days'] = 3; //统计时段
        $sort = strtolower($params['sort'] ?? 'pv,desc');
        $params['sort'] = explode(',', $sort);
        $key = 'pdd_wechat_mini:keyword_search_make_title:' . md5(serialize($params));
        $data = $this->pika->get($key);
        if ($data) {
            return $this->successResponse(json_decode($data, true));
        }
        $res = (new GoodsService)->getGoodsDetail($goods_id, $request);
        if (array_key_exists('buildRequest', $res)) {
            return $this->errorResponse($res);
        }
        $items = [];
        if ($res) {
            $goods_name = $res->goods_name;
            $items = (new TitleAnalysis)->titleAnalysis($goods_name);
        }

        $this->pika->set($key, json_encode($items, 320), 'EX', strtotime(date('Y-m-d 23:59:59')) - time());
        return $this->successResponse($items);
    }

    private function getProducts($goodsIds, $date, $params, $limit = 1000)
    {
        $key = md5(serialize($params) . serialize($goodsIds ?? [])) . $limit . ':' . $date . ':v4';
        $data = json_decode($this->pika->get($key)?:'',true);
        if (!empty($data)) {
            return $data;
        }
        $is_filter = $params['is_filter'] ?? true;

        $data = [];
        $type = get_data_change_switch("wecaht_mini_index_search_type_switch");
        if ($type == 2) {
            if ($goodsIds) {
                $where['goods_id'] = ['goods_id', 'in', $goodsIds];
            } else {
                $where[1] = ['day_sale', '>=', 5000];
            }
            $where[1] = ['day_sale', '>', 0];
            $where[2] = ['day_sale', '<', 11800];
            if (isset($params['cat_info']) && !empty($params['cat_info']['catid'])) {
                if($params['cat_info']['level']==0){
                    $mainCatItems = (new KeywordHandle)->getMainCategoryDatas([$params['cat_info']['catid']],true);
                    $catIds = $mainCatItems[$params['catid']]['bind_admin_class'] ?? [];
                    if ($catIds) {
                        $where['cat_id'] = ['cat_id_1', 'in', $catIds];
                    }
                }else{
                    $where['cat_id_' . $params['cat_info']['level']] = ['cat_id_' . $params['cat_info']['level'], 'term', $params['cat_info']['catid']];
                }
            }
            else {
                $where['cat_id_3'] = ['cat_id_3', '>', 0];
            }
            if ($goodsIds) {
                $where['goods_id'] = ['goods_id', 'in', (array)$goodsIds];
            }
            if ($params['type'] == 2) {
                //飙升商品
                $order[] = ["field" => "fall_rise", "sort" => "desc"];
                $where[1] = ['day_sale', '>', 100];
                $where[3] = ['week_rise', '>', 1.5];
            } else {
                //热销商品
                $order[] = ["field" => "day_sale", "sort" => "desc"];
                $where[3] = ['week_rise', '>', 1.2];
            }
            if (array_key_exists('goods_id', $where)) {
                unset($where[1], $where[2], $where[3]);
            }
            $where = array_values($where);
            $goodsData = (new GoodsService)->getGoodsForTidb($where, $date, 1000, $order, 0, $is_filter);

            $goodsIds = [];
            foreach ($goodsData as $v) {
                $data[$v["goods_id"]] = $v;
                $goodsIds[] = $v['goods_id'];
            }
        } else {
            if (!empty($goodsIds)) {
                $query = SpuRank::date($date)->whereIn('goods_id', array_unique($goodsIds));
            } else {
                $query = SpuRank::date($date)
                    ->where('day_sale', '>', '0')
                    ->where('day_sale', '<', '11800') //避免展示销量异常数据
                    ->body(['collapse' => ['field' => 'goods_id']]);
                if (isset($params['cat_info']) && !empty($params['cat_info']['catid'])) {
                    $query->where('cat_id_' . $params['cat_info']['level'], $params['cat_info']['catid']);
                } elseif (isset($params['catid']) && !empty($params['catid'])) {
                    $mainCatItems = (new KeywordHandle)->getMainCategoryDatas([$params['catid']]);
                    $catIds = $mainCatItems[$params['catid']]['bind_admin_class'] ?? [];
                    if ($catIds) {
                        $query->whereIn('cat_id_1', $catIds);
                    }
                } else {
                    $query->where('cat_id_3', '>', 0); //过滤无类目商品
                }
                if ($goodsIds) {
                    $query->whereIn('goods_id', (array)$goodsIds);
                } else {
                    if ($params['type'] == 2) {
                        //飙升商品
                        $query = $query->where('day_sale', '>', 100)->where('script', 'script',  "doc['week_sale'].value >( doc['day_sale'].value * 1.5)");
                    } else {
                        //热销商品
                        $query = $query->where('script', 'script',  "doc['week_sale'].value >( doc['day_sale'].value * 1.2)");
                    }
                }
                if ($params['type'] == 2) {
                    $query = $query->orderBy('fall_rise', 'desc');
                } else {
                    $query = $query->orderBy('day_sale', 'desc');
                }
            }

            $queryV1 = '';
            if ($goodsIds) {
                $spuRank = $query->take($limit)->get();
            } else {
                $queryV1 = clone $query;
                $spuRank = $query->where('fall_rise', '<', 50000)->where('fall_rise', '>', 200)->take($limit)->get();
                if ($spuRank->count() < 50) {
                    $spuRank = $queryV1->where('day_sale', '<', 50000)->where('day_sale', '>', 1)->take($limit)->get();
                }
            }
            unset($queryV1, $query);
            $items = $spuRank->toArray(); //(new GoodsService)->getMultiRecentSaleV4($query, 100, $date);
            $data = array_column($items, null, 'goods_id');
            $goodsIds = [];
            foreach ($spuRank->toArray() as $v) {
                unset($v['_index'], $v['_type'], $v['_id'], $v['_score']);
                $data[$v['goods_id']] = $v;
                $goodsIds[] = $v['goods_id'];
            }
        }
        $mallIds = $catIds = $goodsData = [];
        if ($data) {
            $goodsInfos = PddGoodsSimple::whereIn('goods_id', (array)$goodsIds)->where('is_on_sale', 1)->take(count($goodsIds))->get();

            foreach ($goodsInfos as $goodsInfo) {
                if (isset($data[$goodsInfo->goods_id])) {
                    $data[$goodsInfo->goods_id]['price'] = floatval($goodsInfo->price ?: $goodsInfo->max_group_price);
                    $data[$goodsInfo->goods_id]['thumb_url'] = $goodsInfo->thumb_url;
                    $data[$goodsInfo->goods_id]['goods_name'] = $goodsInfo->goods_name;
                    $data[$goodsInfo->goods_id]['cat_id_3'] = $data[$goodsInfo->goods_id]['cat_id_3'] ?: $goodsInfo->cat_id_3;
                    $data[$goodsInfo->goods_id]['mall_id'] = $goodsInfo->mall_id;
                    $data[$goodsInfo->goods_id]['day_amount'] = round($data[$goodsInfo->goods_id]['price'] * $data[$goodsInfo->goods_id]['day_sale'], 2);
                    $data[$goodsInfo->goods_id]['mall_name'] = '';
                    $data[$goodsInfo->goods_id]['mall_logo'] = '';
                    $mallIds[] = $goodsInfo->mall_id;
                    $catIds[] = $data[$goodsInfo->goods_id]['cat_id_3'];

                    $goodsData[] = $data[$goodsInfo->goods_id];
                }
            }
        }
        $mallList = [];
        if ($mallIds) {
            $mallList = (new MallService())->getMallInfo($mallIds);
            $mallList = array_column($mallList, null, 'mall_id');
        }
        $catInfo = [];
        if ($catIds) {
            $catInfo = MmsCategory::getCategoryDatas($catIds, 3);
        }
        if($goodsData){
            foreach ($goodsData as &$val) {
                $val['mall_name'] = $mallList[$val['mall_id']]['mall_name'] ?? '';
                $val['mall_logo'] = $mallList[$val['mall_id']]['logo'] ?? '';
                $val['category_name'] = $catInfo[$val['cat_id_3']]['cat_name'] ?? '';
            }
            $this->pika->set($key, json_encode($goodsData, 320), 'EX', strtotime(date('Y-m-d 23:59:59')) - time()); //凌晨一刷
        }
        return $goodsData;
    }
}
